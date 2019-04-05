import asyncio
import logging
import random
import operator
import functools
from collections import defaultdict
from heapq import nsmallest
from hashlib import sha256

import msgpack
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as PrivateKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey as PublicKey

from qadom.rpcudp import RPCProtocol


log = logging.getLogger(__name__)


REPLICATION_DEFAULT = 5  # TODO: increase
REPLICATION_MAX = 20


async def gather(mapping, **kwargs):
    """Like asyncio.gather but takes dict as argument"""
    coroutines = list(mapping.values())
    results = await asyncio.gather(*coroutines, **kwargs)
    return dict(zip(mapping.keys(), results))


def make_uid():
    """Create a wanna-be unique identifier. Return an integer."""
    return random.getrandbits(UID_LENGTH)


def nearest(k, peers, uid):
    """Return K nearest to to UID peers in PEERS according to XOR"""
    # XXX: It only works with len(peers) < 10^6 more than that count
    # of peers and the time it takes to compute the nearest peers will
    # timeout after 5 seconds on the other side. See RPCProtocol and
    # Peer.peers.
    return nsmallest(k, peers, key=functools.partial(operator.xor, uid))


def pack(integer):
    """Returns a bytes representation of integer in network order"""
    return integer.to_bytes(32, byteorder='big')


def unpack(bytes):
    """Returns an integer"""
    return int.from_bytes(bytes, byteorder='big')


def hash(bytes):
    """Return the sha256 of BYTES as an integer"""
    return unpack(sha256(bytes).digest())


UID_LENGTH = len(bin(hash(b''))) - 2
print(UID_LENGTH)


def iter_roots(count):
    for i in range(count):
        yield 2**i


class _Peer:


    def __init__(self, uid, private_key, replication=REPLICATION_DEFAULT):
        assert replication <= REPLICATION_MAX
        # equivalent to k in kademlia, also used as alpha. It specify the
        # how many peers are returned in peers, how many peers will
        # receive store calls to store a value and also the number of
        # peers that are contacted when looking up peers in peers.
        self.replication = replication
        # bag associates a key with a set of key.  This can be freely
        # set by peers in the network and allows to link a well known
        # key to other keys. It is inspired from gnunet-fs keywords
        # feature. See 'Peer.add' and 'Peer.search'.
        self._bag = defaultdict(set)
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

        # for use with namespace

        # ed25519 private key (that includes the public key)
        self._private_key = private_key
        # store key/value pairs per public_key
        self._namespace = defaultdict(dict)

    def __repr__(self):
        return '<_Peer "%r">' % self._uid

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
        self._blacklist.add(address[0])

    async def listen(self, port, interface='0.0.0.0'):
        """Start listening on the given port.

        Provide interface="::" to accept ipv6 address.

        """
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(RPCProtocol, local_addr=(interface, port))
        self._transport, self._protocol = await listen
        # register remote procedures
        self._protocol.register(self.ping)
        self._protocol.register(self.peers)
        self._protocol.register(self.value)
        self._protocol.register(self.store)
        self._protocol.register(self.add)
        self._protocol.register(self.search)
        self._protocol.register(self.namespace_get)
        self._protocol.register(self.namespace_set)

    async def bootstrap(self, address):
        """Add address to the list of peers.

        Send a ping to ADDRESS and add it with its uid as in the list of
        known peers.

        """
        log.debug('boostrap at %r', address)
        uid = await self._protocol.rpc(address, 'ping', pack(self._uid))
        uid = unpack(uid)
        assert self._peers.get(uid) is None or self._peers.get(uid) == address
        self._peers[uid] = address
        assert self._addresses.get(address) is None or self._addresses.get(address) == uid
        self._addresses[address] = uid
        await self._connect()

    async def _connect(self):
        # XXX: This is a tentative to populate the routing table with
        # enough nodes to cover 2^UID_LENGTH space and avoid lookup
        # KeyError because there is part of the space that self can
        # not reach

        # TODO: optimize and make it part of Peer.refresh()
        for root in iter_roots(UID_LENGTH):
            try:
                await self._get(root)
            except KeyError:
                pass

    # helper

    async def _is_near(self, uid):
        """Verify in the routing table that self is near the key"""
        peers = await self.peers((None, None), pack(uid))
        peers = [self._addresses[address] for address in peers]
        # XXX: MUST respect REPLICATION_MAX globally otherwise peers
        # will get blacklisted for no good reasons!
        peers = nearest(REPLICATION_MAX, peers, uid)
        high = peers[-1] ^ uid
        current = self._uid ^ uid
        out = high > current
        return out

    # remote procedures

    async def ping(self, address, uid):
        """Remote procedure that register the remote and returns the uid"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return pack(self._uid)
        uid = unpack(uid)
        log.debug("[%r] ping uid=%r from %r", self._uid, uid, address)
        self._peers[uid] = address
        self._addresses[address] = uid
        return pack(self._uid)

    async def peers(self, address, uid):
        """Remote procedure that returns peers that are near UID"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return [random.randint(0, 2**UID_LENGTH) for x in range(self.replication)]
        # The code is riddle with unpack/pack calls because Peer
        # stores key/uid as integer and msgpack doesn't accept such
        # big integers hence it is required to pass them as bytes.
        uid = unpack(uid)
        log.debug("[%r] find peers uid=%r from %r", self._uid, uid, address)
        # XXX: if this takes more than 5 seconds (see RPCProtocol) it
        # will timeout in the other side.
        uids = nearest(self.replication, self._peers.keys(), uid)
        out = [self._peers[x] for x in uids]
        return out

    # dict procedures (vanilla dht api)

    async def value(self, address, key):
        """Remote procedure that returns the associated value or peers that
        are near KEY"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return (b'PEERS', [random.randint(0, 2**UID_LENGTH) for x in range(self.replication)])
        log.debug("[%r] find value key=%r from %r", self._uid, key, address)
        try:
            return (b'VALUE', self._storage[unpack(key)])
        except KeyError:
            out = await self.peers((None, None), key)
            return (b'PEERS', out)

    async def store(self, address, value):
        """Remote procedure that stores value locally with its digest as
        key"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return True
        log.debug("[%r] store from %r", self._uid, address)
        uid = hash(value)

        ok = await self._is_near(uid)
        if ok:
            self._storage[uid] = value
            return True
        else:
            log.warning('[%r] received a value that is too far, by %r', self._uid, address)
            return False

    # bag procedures

    async def add(self, address, key, value):
        """Remote procedure that adds VALUE to the list of uid at KEY"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return True

        log.debug("[%r] add key=%r value=%r from %r", self._uid, key, value, address)
        key = unpack(key)
        value = unpack(value)
        if key > 2**UID_LENGTH or value > 2**UID_LENGTH:
            log.warning('[%r] received a add that is invalid, from %r', self._uid, address)
            return False

        ok = await self._is_near(key)
        if ok:
            self._bag[key].add(value)
            return True
        else:
            log.warning('[%r] received a add that is too far, by %r', self._uid, address)
            return False

    async def search(self, address, uid):
        """Remote procedure that returns values associated with KEY if any,
        otherwise return peers near KEY"""
        log.debug("[%r] search uid=%r from %r", self._uid, uid, address)
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return (b'PEERS', [random.randint(0, 2**UID_LENGTH) for x in range(self.replication)])

        uid = unpack(uid)
        if uid in self._bag:
            values = [pack(v) for v in self._bag[uid]]
            return (b'VALUES', values)
        else:
            peers = await self.peers((None, None), pack(uid))
            return (b'PEERS', peers)

    # namespace procedures

    async def namespace_set(self, address, public_key, key, value, signature):
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return True
        log.debug('namespace_set form %r', address)
        uid = hash(msgpack.packb((public_key, key)))

        ok = await self._is_near(uid)
        if ok:
            public = PublicKey.from_public_bytes(public_key)
            try:
                public.verify(signature, msgpack.packb((key, value)))
            except InvalidSignature:
                self.blacklist(address)
                log.warning('[%r] invalid signature from %r', self._uid, address)
                # XXX: pretend everything is ok
                return True
            else:
                # store it
                self._namespace[public_key][unpack(key)] = value
                return True
        else:
            log.warning('[%r] received namespace_set that is too far, by %r', self._uid, address)
            return False

    async def namespace_get(self, address, public_key, key):
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return (b'PEERS', [random.randint(0, 2**UID_LENGTH) for x in range(self.replication)])

        if public_key in self._namespace:
            try:
                return (b'VALUE', self._namespace[public_key][unpack(key)])
            except KeyError:
                pass
        # key not found, return nearest peers
        uid = hash(msgpack.packb((public_key, key)))
        peers = await self.peers((None, None), pack(uid))
        return (b'PEERS', peers)

    # helpers

    async def _welcome_peers(self, addresses):
        queries = dict()
        for address in addresses:
            query = self._protocol.rpc(address, 'ping', pack(self._uid))
            queries[address] = query
        responses = await gather(queries, return_exceptions=True)
        for (address, maybe_uid) in responses.items():
            if isinstance(maybe_uid, Exception):
                continue
            uid = unpack(maybe_uid)
            self._peers[uid] = address
            self._addresses[address] = uid

    async def _reach(self, uid):
        log.debug('reach uid=%r', uid)
        try:
            return self._peers[uid]
        except KeyError:
            try:
                # try to reach node UID
                await self.get(uid)  # TODO: optimize
            except KeyError:
                pass
            finally:
                return self._peers[uid]

    # local methods

    async def get_at(self, key, uid):
        """Get the value associated with KEY at peer with identifier UID"""
        log.debug('get_at key=%r uid=%r', key, uid)
        try:
            peer = await self._reach(uid)
        except KeyError as exc:
            raise KeyError(key) from exc

        out = await self._protocol.rpc(peer, 'value', pack(key))
        if out[0] == b'VALUE':
            return out[1]
        else:
            raise KeyError(key)

    async def get(self, key):
        """Local method to fetch the value associated with KEY

        KEY must be an integer below 2^256"""
        assert key <= 2**UID_LENGTH
        try:
            return self._storage[key]
        except KeyError:
            out = await self._get(key)
            return out

    async def _get(self, key):
        """Fetch the value associated with KEY from the network"""
        uid = pack(key)
        queried = set()
        while True:
            # retrieve the k nearest peers and remove already queried peers
            peers = await self.peers((None, None), uid)
            peers = [address for address in peers if address not in queried]
            # no more peer to query, the key is not found in the dht
            if not peers:
                raise KeyError(unpack(uid))
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, 'value', uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b'VALUE':
                    value = response[1]
                    if hash(value) == unpack(uid):
                        self._storage[unpack(uid)] = value
                        return value
                    else:
                        log.warning('[%r] bad value returned from %r', self._uid, address)
                        self.blacklist(address)
                        continue
                elif response[0] == b'PEERS':
                    await self._welcome_peers(response[1])
                else:
                    self.blacklist(address)
                    log.warning('[%r] unknown response %r from %r', self._uid, response[0], address)

    async def set(self, value):
        """Store VALUE in the network.

        Return the uid with which it is associated aka. sha256 integer representation."""
        if len(value) > (8192 - 28):  # datagram max size minus
                                      # "header", see RPCProtocol.
            raise ValueError('value too big')
        uid = pack(hash(value))
        # unlike kademlia store value locally
        self._storage[unpack(uid)] = value
        # find the nearest peers and call store rpc
        queried = set()
        while True:
            # find peers and remove already queried peers
            peers = await self.peers((None, None), uid)
            peers = [address for address in peers if address not in queried]
            # no more peer to query, the nearest peers in the network
            # are known
            if not peers:
                peers = await self.peers((None, None), uid)
                queries = [self._protocol.rpc(address, 'store', value) for address in peers]
                # TODO: make sure replication is fullfilled
                await asyncio.gather(*queries, return_exceptions=True)
                return unpack(uid)
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, 'peers', uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                await self._welcome_peers(response)

    # key local method

    async def bag(self, key, value=None):
        """Bag search and publish.

        If VALUE is set, it will append VALUE to KEY in the network.
        If VALUE is NOT set, it will lookup uid associated with KEY in
        the network.

        Both VALUE and KEY must be integers below 2^256.

        """
        if value is None:
            out = await self._search(key)
            return out
        else:
            await self._add(key, value)

    async def bag_at(self, key, uid):
        try:
            peer = await self._reach(uid)
        except KeyError as exc:
            raise KeyError(key) from exc

        response = await self._protocol.rpc(peer, 'search', pack(key))
        if response[0] == b'VALUES':
            out = {unpack(x) for x in response[1]}
            return out
        else:
            raise KeyError(key)

    async def _add(self, key, value):
        """Publish VALUE at KEY"""
        # unlike kademlia store locally
        self._bag[key].add(value)
        # proceed
        uid = pack(key)
        value = pack(value)
        # find the nearest peers and call append rpc
        queried = set()
        while True:
            # find peers and remove already queried peers
            peers = await self.peers((None, None), uid)
            peers = [address for address in peers if address not in queried]
            # no more peer to query, the nearest peers in the network
            # are known
            if not peers:
                peers = await self.peers((None, None), uid)
                queries = [self._protocol.rpc(address, 'add', uid, value) for address in peers]
                # TODO: make sure replication is fullfilled
                await asyncio.gather(*queries, return_exceptions=True)
                return
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, 'peers', uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                await self._welcome_peers(response)

    async def _search(self, key):
        """Search values associated with KEY"""
        out = set()
        if key in self._bag:
            try:
                out = self._bag[key]
            except KeyError:
                pass

        key = pack(key)
        queried = set()
        while True:
            # retrieve the k nearest peers and remove already queried peers
            peers = await self.peers((None, None), key)
            peers = [address for address in peers if address not in queried]
            # no more peer to query
            if not peers:
                # store results locally
                self._bag[key] = self.bag[key].union(out)
                return out
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, 'search', key)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b'VALUES':
                    values = set([unpack(x) for x in response[1]])
                    out = out.union(values)
                elif response[0] == b'PEERS':
                    await self._welcome_peers(response[1])
                else:
                    self.blacklist(address)
                    log.warning('[%r] unknown response %r from %r', self._uid, response[0], address)


    # namespace local method

    async def namespace(self, key, value=None, public_key=None, signature=None):
        assert key <= 2**UID_LENGTH
        if value is None:
            assert isinstance(public_key, bytes)
            assert isinstance(signature, bytes)
            out = await self._namespace_get(public_key, key, signature)
            return out
        else:
            assert isinstance(value, bytes)
            assert len(value) < 8000  # TODO: compute the real max size
            out = await self._namespace_set(key, value)
            return out

    async def namespace_at(self, key, public_key, signature, uid):
        try:
            peer = await self._reach(uid)
        except KeyError as exc:
            raise KeyError((public_key, key)) from exc

        key = pack(key)
        response = await self._protocol.rpc(peer, 'namespace_get', public_key, key)
        if response[0] == b'VALUE':
            value = response[1]
            public_key_object = PublicKey.from_public_bytes(public_key)
            payload = msgpack.packb((key, value))
            try:
                public_key_object.verify(signature, payload)
            except InvalidSignature as exc:
                self.warning('invalid namespace set from %r', address)
                self.blacklist(address)
                raise KeyError((public_key, unpack(key))) from exc
            else:
                self._namespace[public_key][unpack(key)] = value
                return value
        else:
            raise KeyError((public_key, unpack(key)))

    async def _namespace_get(self, public_key, key, signature):
        # check local namespace
        if public_key in self._namespace:
            try:
                out = self._namespace[public_key][key]
            except KeyError:
                pass
            else:
                return out
        # proceed
        key = pack(key)
        uid = pack(hash(msgpack.packb((public_key, key))))
        queried = set()
        while True:
            # retrieve the k nearest peers and remove already queried peers
            peers = await self.peers((None, None), uid)
            peers = [address for address in peers if address not in queried]
            # no more peer to query, the key is not found
            if not peers:
                raise KeyError((unpack(public_key), unpack(key)))
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, 'namespace_get', public_key, key)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b'VALUE':
                    value = response[1]
                    public_key_object = PublicKey.from_public_bytes(public_key)
                    payload = msgpack.packb((key, value))
                    try:
                        public_key_object.verify(signature, payload)
                    except InvalidSignature:
                        self.warning('invalid namespace get from %r', address)
                        self.blacklist(address)
                        continue
                    else:
                        self._namespace[public_key][unpack(key)] = value
                        return value
                elif response[0] == b'PEERS':
                    await self._welcome_peers(response[1])
                else:
                    self.blacklist(address)
                    log.warning('[%r] unknown response %r from %r', self._uid, response[0], address)

    async def _namespace_set(self, key, value):
        """Publish VALUE at KEY"""
        # compute identifier of the node where to store that (public_key, key, value)
        public_key = self._private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        # unlike kademlia store locally
        self._namespace[public_key][key] = value
        # proceed
        key = pack(key)
        uid = pack(hash(msgpack.packb((public_key, key))))
        # find the nearest peers and call namespace_set rpc
        queried = set()
        while True:
            # find peers and remove already queried peers
            peers = await self.peers((None, None), uid)
            peers = [address for address in peers if address not in queried]
            # no more peer to query, the nearest peers in the network
            # are known
            if not peers:
                # sign pair
                payload = msgpack.packb((key, value))
                signature = self._private_key.sign(payload)
                # call rpc in nearest peers
                peers = await self.peers((None, None), uid)
                queries = []
                for address in peers:
                    query = self._protocol.rpc(
                        address,
                        'namespace_set',
                        public_key,
                        key,
                        value,
                        signature
                    )
                    queries.append(query)
                # TODO: make sure replication is fullfilled
                await asyncio.gather(*queries, return_exceptions=True)
                return signature
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, 'peers', uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                await self._welcome_peers(response)


async def make_peer(uid, port, private_key=None):
    """Create a peer at PORT with UID as identifier"""
    if private_key is None:
        private_key = PrivateKey.generate()
    peer = _Peer(uid, private_key)
    await peer.listen(port)
    return peer
