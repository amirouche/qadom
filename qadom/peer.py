import asyncio
import logging
from time import time
from heapq import nsmallest
from hashlib import sha256

from qadom.rpcudp import RPCProtocol


log = logging.getLogger(__name__)


def make_uid():
    return int(sha256(str(time()).encode('utf8')).hexdigest(), 16)


def nearest(k, peers, uid):
    return nsmallest(k, peers, key=lambda x: x ^ uid)


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
        uid = hex(self._uid)[2:].encode('utf8')
        uid = await self._protocol.rpc(address, 'ping', uid)
        await self.ping(address, uid)

    # remote procedures

    async def ping(self, address, uid):
        log.debug("ping from %r uid=%r", address, uid)
        uid = int(uid.decode('utf8'), 16)
        self._peers[uid] = address
        return hex(self._uid)[2:].encode('utf8')

    async def find_peers(self, address, uid):
        log.debug("find peers from %r uid=%r", address, uid)
        uid = int(uid.decode('utf8'), 16)
        uids = nearest(self.CONCURRENCY, self._peers.keys(), uid)
        out = [(hex(uid)[2:].encode('utf8'), self._peers[uid]) for uid in uids]
        return out

    async def find_value(self, address, key):
        log.debug("find value from %r key=%r", address, key)
        key2 = int(key.decode('utf8'), 16)
        try:
            return ('VALUE', self._storage[key2])
        except KeyError:
            out = await self.find_peers(None, key)
            return ('PEERS', out)

    async def store(self, address, value):
        log.debug("store from %r", address)
        key = int(sha256(value).hexdigest(), 16)
        self._storage[key] = value
        return True

    # local methods

    async def get(self, key):
        local = int(key, 16)
        try:
            return self._storage[key]
        except KeyError:
            out = await self._get(key)
            return out

    async def _get(self, key):
        key = key.encode('utf8')
        queried = set()
        while True:
            # retrieve the k nearest peers
            peers = await self.find_peers(None, key)
            # remove already queried peers
            to_remove = []
            for (uid, address) in peers:
                if uid in queried:
                    to_remove.append((uid, address))
            for item in to_remove:
                peers.remove(item)
            # no more peer to query, the key is not found in the dht
            if not peers:
                raise KeyError(key)
            # query selected peers
            queries = []
            for _, address in peers:
                query = self._protocol.rpc(address, 'find_value', key)
                queries.append(query)
            responses = await asyncio.gather(*queries, return_exceptions=True)
            for (response, (peer, address)) in zip(responses, peers):
                queried.add(peer)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b'VALUE':
                    value = response[1]
                    key = int(key.decode('utf8'), 16)
                    self._storage[key] = value
                    return value
                elif response[0] == b'PEERS':
                    for peer, address in response[1]:
                        await self.ping(tuple(address), peer)
                else:
                    log.warning('unknown response %r from %r', response[0], address)

    async def set(self, value):
        # unlike kademlia we store our own value
        digest = sha256(value.encode('utf8')).hexdigest()
        key = int(digest, 16)
        self._storage[key] = value
        # store in the dht, find the nearest peers and call store rpc
        key = digest.encode('utf8')
        queried = set()
        while True:
            peers = await self.find_peers(None, key)
            # remove already queried peers
            to_remove = []
            for (uid, address) in peers:
                if uid in queried:
                    to_remove.append((uid, address))
            for item in to_remove:
                peers.remove(item)
            # no more peer to query, the nearest peers in the network
            # are known
            if not peers:
                peers = await self.find_peers(None, key)
                for (peer, address) in peers:
                    await self._protocol.rpc(address, 'store', value)
                return digest
            # query selected peers
            queries = []
            for _, address in peers:
                query = self._protocol.rpc(tuple(address), 'find_peers', key)
                queries.append(query)
            responses = await asyncio.gather(*queries, return_exceptions=True)
            for (response, (peer, address)) in zip(responses, peers):
                queried.add(peer)
                if isinstance(response, Exception):
                    continue
                for peer, address in response:
                    await self.ping(tuple(address), peer)


async def make_peer(uid, port):
    peer = _Peer(uid)
    await peer.listen(port)
    return peer
