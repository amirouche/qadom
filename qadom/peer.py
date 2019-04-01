import asyncio
import logging
from time import time
from heapq import nsmallest
from hashlib import sha256

from qadom.rpcudp import RPCProtocol


log = logging.getLogger(__name__)


def make_uid():
    return unpack(sha256(str(time()).encode('utf8')).digest())


def nearest(k, peers, uid):
    return nsmallest(k, peers, key=lambda x: x ^ uid)


def pack(integer):
    return integer.to_bytes(32, byteorder='big')

def unpack(bytes):
    return int.from_bytes(bytes, byteorder='big')


class _Peer:

    CONCURRENCY = 20

    def __init__(self, uid):
        self._uid = uid
        self._storage = dict()
        self._peers = dict()
        self._transport = None
        self._protocol = None

    def close(self):
        self._transport.close()

    async def listen(self, port, interface='0.0.0.0'):
        """Start listening on the given port.

        Provide interface="::" to accept ipv6 address

        """
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(RPCProtocol, local_addr=(interface, port))
        self._transport, self._protocol = await listen
        # register remote procedures
        self._protocol.register(self.ping)
        self._protocol.register(self.find_peers)
        self._protocol.register(self.find_value)
        self._protocol.register(self.store)

    async def bootstrap(self, address):
        uid = await self._protocol.rpc(address, 'ping', pack(self._uid))
        await self.ping(address, uid)

    # remote procedures

    async def ping(self, address, uid):
        log.debug("ping from %r uid=%r", address, uid)
        uid = unpack(uid)
        self._peers[uid] = address
        return pack(self._uid)

    async def find_peers(self, address, uid):
        log.debug("find peers from %r uid=%r", address, uid)
        uid = unpack(uid)
        uids = nearest(self.CONCURRENCY, self._peers.keys(), uid)
        out = [(pack(uid), self._peers[uid]) for uid in uids]
        return out

    async def find_value(self, address, key):
        log.debug("find value from %r key=%r", address, key)
        try:
            return (b'VALUE', self._storage[unpack(key)])
        except KeyError:
            out = await self.find_peers(None, key)
            return (b'PEERS', out)

    async def store(self, address, value):
        log.debug("store from %r", address)
        key = unpack(sha256(value).digest())
        self._storage[key] = value
        return True

    # local methods

    async def get(self, key):
        try:
            return self._storage[key]
        except KeyError:
            out = await self._get(key)
            return out

    async def _get(self, key):
        key = pack(key)
        queried = set()
        while True:
            # retrieve the k nearest peers and remove already queried peers
            peers = await self.find_peers(None, key)
            peers = [(uid, address) for (uid, address) in peers if unpack(uid) not in queried]
            # no more peer to query, the key is not found in the dht
            if not peers:
                raise KeyError(unpack(key))
            # query selected peers
            queries = []
            for _, address in peers:
                query = self._protocol.rpc(tuple(address), 'find_value', key)
                queries.append(query)
            responses = await asyncio.gather(*queries, return_exceptions=True)
            for (response, (peer, address)) in zip(responses, peers):
                queried.add(unpack(peer))
                if isinstance(response, Exception):
                    continue
                elif response[0] == b'VALUE':
                    # TODO: check value's sha256
                    value = response[1]
                    self._storage[unpack(key)] = value
                    return value
                elif response[0] == b'PEERS':
                    for peer, address in response[1]:
                        await self.ping(tuple(address), peer)
                else:
                    log.warning('unknown response %r from %r', response[0], address)

    async def set(self, value):
        # unlike kademlia store value locally
        key = unpack(sha256(value).digest())
        self._storage[key] = value
        # store in the dht, find the nearest peers and call store rpc
        key = pack(key)
        queried = set()
        while True:
            # find peers and remove already queried peers
            peers = await self.find_peers(None, key)
            peers = [(uid, address) for (uid, address) in peers if unpack(uid) not in queried]
            # no more peer to query, the nearest peers in the network
            # are known
            if not peers:
                peers = await self.find_peers(None, key)
                queries = [self._protocol.rpc(tuple(address), 'store', value) for (_, address) in peers]
                # TODO: make sure CONCURRENCY is fullfilled
                await asyncio.gather(*queries, return_exceptions=True)
                return unpack(key)
            # query selected peers
            queries = []
            for _, address in peers:
                query = self._protocol.rpc(tuple(address), 'find_peers', key)
                queries.append(query)
            responses = await asyncio.gather(*queries, return_exceptions=True)
            for (response, (peer, address)) in zip(responses, peers):
                queried.add(unpack(peer))
                if isinstance(response, Exception):
                    continue
                for peer, address in response:
                    await self.ping(tuple(address), peer)


async def make_peer(uid, port):
    peer = _Peer(uid)
    await peer.listen(port)
    return peer
