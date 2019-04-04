import asyncio
import logging
import random
import operator
import functools
from collections import defaultdict
from heapq import nsmallest
from hashlib import sha256

from qadom.rpcudp import RPCProtocol


log = logging.getLogger(__name__)


REPLICATION_DEFAULT = 5  # TODO: increase
REPLICATION_MAX = 20


def make_uid():
    """Create a wanna-be unique identifier. Return an integer."""
    return random.getrandbits(256)


def nearest(k, peers, uid):
    """Return K nearest to to UID peers in PEERS according to XOR"""
    # XXX: It only works with len(peers) < 10^6 more than that count
    # of peers and the time it takes to compute the nearest peers will
    # timeout after 5 seconds on the other side. See RPCProtocol and
    # Peer.find_peers.
    return nsmallest(k, peers, key=functools.partial(operator.xor, uid))


def pack(integer):
    """Returns a bytes representation of integer in network order"""
    return integer.to_bytes(32, byteorder='big')


def unpack(bytes):
    """Returns an integer"""
    return int.from_bytes(bytes, byteorder='big')


def digest(bytes):
    """Return the sha256 of BYTES as an integer"""
    return unpack(hashlib.sha256(bytes).digest())


class _Peer:

    # equivalent to k in kademlia, also used as alpha. It specify the
    # how many peers are returned in find_peers, how many peers will
    # receive store calls to store a value and also the number of
    # peers that are contacted when looking up peers in find_peers.

    def __init__(self, uid, replication=REPLICATION_DEFAULT):
        assert replication <= REPLICATION_MAX
        self.replication = replication
        # keys associates a key with a list of key.  This can be
        # freely set by peers in the network and allows to link a well
        # known key to other keys. It is inspired from gnunet-fs
        # keywords. See 'Peer.append' and 'Peer.search'.
        self._keys = defaultdict(list)
        # peers stores the equivalent of the kademlia routing table
        # aka. kbuckets. uid/key to address mapping.
        self._peers = dict()
        # address to uid/key mapping
        self._addresses = dict()
        # blacklist misbehaving nodes. Stores uid/key.
        self._blacklist = set()
        # RPCProtocol set in Peer.listen
        self._protocol = None
        # storage associate a key to a value.  The key must always be
        # the unpacked sha256 of the value.
        self._storage = dict()
        # Set in Peer.listen
        self._transport = None
        # uid (pronouced 'weed') is the identifier of the peer in the
        # overlay network.  It is self-assigned and must be globally
        # unique otherwise some Bad Things can happen. The uid specify
        # loosly depending on the network topology which keys that
        # peer is responsible for. uid must be in the same space as
        # keys that is less than 2^256.
        self._uid = uid

    def close(self):
        self._transport.close()

    def blacklist(self, address):
        try:
            uid = self._addresses[address]
        except KeyError:
            pass
        else:
            del self._addresses[address]
            del self._peers[uid]
        self._blacklist.add(uid)

    async def listen(self, port, interface='0.0.0.0'):
        """Start listening on the given port.

        Provide interface="::" to accept ipv6 address.

        """
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(RPCProtocol, local_addr=(interface, port))
        self._transport, self._protocol = await listen
        # register remote procedures
        self._protocol.register(self.ping)
        self._protocol.register(self.find_peers)
        self._protocol.register(self.find_value)
        self._protocol.register(self.store)
        self._protocol.register(self.append)
        self._protocol.register(self.search)

    async def bootstrap(self, address):
        """Add address to the list of peers.

        Send a ping to ADDRESS and add it with its uid as in the list of
        known peers.

        """
        uid = await self._protocol.rpc(address, 'ping', pack(self._uid))
        await self.ping(address, uid)

    # remote procedures

    async def ping(self, address, uid):
        """Remote procedure that register the remote and returns the uid"""
        uid = unpack(uid)
        log.debug("ping uid=%r from %r", uid, address)
        self._peers[uid] = address
        self._addresses[address] = uid
        return pack(self._uid)

    async def find_peers(self, address, uid):
        """Remote procedure that returns peers that are near UID"""
        # The code is riddle with unpack/pack calls because Peer
        # stores key/uid as integer and msgpack doesn't accept such
        # big integers hence it is required to pass them as bytes.
        uid = unpack(uid)
        log.debug("find peers uid=%r from %r", uid, address)
        # XXX: if this takes more than 5 seconds (see RPCProtocol) it
        # will timeout in the other side.
        uids = nearest(self.replication, self._peers.keys(), uid)
        out = [(pack(uid), self._peers[uid]) for uid in uids]
        return out

    # dht procedures

    async def find_value(self, address, key):
        """Remote procedure that returns the associated value or peers that
        are near KEY"""
        log.debug("find value from %r key=%r", address, key)
        try:
            return (b'VALUE', self._storage[unpack(key)])
        except KeyError:
            out = await self.find_peers(None, key)
            return (b'PEERS', out)

    async def store(self, address, value):
        """Remote procedure that stores value locally with its digest as
        key"""
        log.debug("store from %r", address)
        key = digest(value)
        # check that peer is near the key
        peers = self.find_peers(None, pack(key))
        peers = nearest(REPLICATION_MAX, peers, key)
        high = peers[-1] ^ key
        current = self._uid ^ key
        if current > high:
            log.warning('received a value that is too far from %r', address)
            self.blacklist(address)
            # XXX: pretend the value was stored
            return True
        else:
            self._storage[key] = value
            return True

    # keys procedures

    async def append(self, address, key, value):
        """Remote procedure that appends VALUE to the list of uid at KEY"""
        log.debug("append key=%r value=%r from %r", key, value, address)
        # TODO: check valid
        self._keys[unpack(key)].append(unpack(value))
        return True

    async def search(self, address, key):
        """Remote procedure that returns values associated with KEY if any,
        otherwise return peers near KEY"""
        log.debug("search key=%r from %r", key, address)
        key = unpack(key)
        if key in self._keys:
            values = [pack(v) for v in self._keys[key]]
            return (b'VALUES', values)
        else:
            peers = await self.find_peers(None, pack(key))
            return (b'PEERS', peers)

    # local methods

    async def get(self, key):
        """Local method to fetch the value associated with KEY

        KEY must be an integer below 2^256"""
        try:
            return self._storage[key]
        except KeyError:
            out = await self._get(key)
            return out

    async def _get(self, key):
        """Fetch the value from the network"""
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
        """Store VALUE in the network.

        Return the uid where the value is stored."""
        if len(value) > (8192 - 28):  # datagram max size minus
                                      # "header", see RPCProtocol.
            raise ValueError('value too big')
        key = unpack(sha256(value).digest())
        # unlike kademlia store value locally
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
                # TODO: make sure replication is fullfilled
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

    async def key(self, key, value=None):
        """Key search and publish.

        If VALUE is set, it will append VALUE to KEY in the network.
        If VALUE is NOT set, it will lookup uid associated with KEY in
        the network.

        Both VALUE and KEY must be integers below 2^256.

        """
        if value is None:
            out = await self._key_search(key)
            return out
        else:
            await self._key_publish(key, value)

    async def _key_publish(self, key, value):
        """Publish VALUE at KEY"""
        key = pack(key)
        value = pack(value)
        # find the nearest peers and call append rpc
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
                queries = [self._protocol.rpc(tuple(x), 'append', key, value) for (_, x) in peers]
                # TODO: make sure replication is fullfilled
                await asyncio.gather(*queries, return_exceptions=True)
                return
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

    async def _key_search(self, key):
        """Search values associated with KEY"""
        out = set()
        queried = set()
        while True:
            # retrieve the k nearest peers and remove already queried peers
            peers = await self.find_peers(None, key)
            peers = [(uid, address) for (uid, address) in peers if unpack(uid) not in queried]
            # no more peer to query
            if not peers:
                return out
            # query selected peers
            queries = []
            for _, address in peers:
                query = self._protocol.rpc(tuple(address), 'search', key)
                queries.append(query)
            responses = await asyncio.gather(*queries, return_exceptions=True)
            for (response, (peer, address)) in zip(responses, peers):
                queried.add(unpack(peer))
                if isinstance(response, Exception):
                    continue
                elif response[0] == b'VALUES':
                    values = set([unpack(x) for x in response[1]])
                    out += values
                elif response[0] == b'PEERS':
                    for peer, address in response[1]:
                        await self.ping(tuple(address), peer)
                else:
                    log.warning('unknown response %r from %r', response[0], address)


async def make_peer(uid, port):
    """Create a peer at PORT with UID as identifier"""
    peer = _Peer(uid)
    await peer.listen(port)
    return peer
