import asyncio
import functools
import logging
import operator
import random
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from heapq import nsmallest
from time import time
from uuid import uuid4

import msgpack
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey as PrivateKey,
)
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PublicKey as PublicKey,
)

import hoply as h
from hoply.memory import MemoryConnexion

from qadom.rpcudp import RPCProtocol


log = logging.getLogger(__name__)


REPLICATION_DEFAULT = 5  # TODO: increase
REPLICATION_MAX = 20


def pick(*names):
    # TODO: move to hoply project
    def wrapped(iterator):
        for bindings in iterator:
            yield tuple(bindings.get(name) for name in names)

    return wrapped


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
    # TODO: bytes.lstrip(b'\x00')
    return integer.to_bytes(32, byteorder="big")


def unpack(bytes):
    """Returns an integer"""
    return int.from_bytes(bytes, byteorder="big")


def hash(bytes):
    """Return the sha256 of BYTES as an integer"""
    return unpack(sha256(bytes).digest())


UID_LENGTH = len(bin(hash(b""))) - 2


def iter_roots(count):
    for i in range(count):
        yield 2 ** i


class _Peer:
    def __init__(self, uid, private_key, hoply, run, replication=REPLICATION_DEFAULT):
        assert replication <= REPLICATION_MAX

        # uid (pronouced 'weed') is the identifier of the peer in the
        # overlay network.  It is self-assigned and must be globally
        # unique otherwise some Bad Things can happen. The uid specify
        # loosly depending on the network topology which keys that
        # peer is responsible for. uid must be in the same space as
        # keys that is less than 2^256.
        self._uid = uid

        # equivalent to k in kademlia, also used as alpha. It specify
        # how many peers are returned in Peer.peers, how many peers
        # will receive store calls to store a value and also the
        # number of peers that are contacted when looking up peers in
        # routing table.
        self._replication = replication

        # blacklist misbehaving nodes. Stores ips. Populated from
        # database in Peer.init.
        self._blacklist = set()

        # peers stores the equivalent of the kademlia routing table
        # aka. kbuckets. Populated from database in Peer.init. uid to
        # address mapping. XXX: if several pears have the same UID,
        # they will overwrite each other. TODO: Maybe use hoply with
        # the in-memory backend to store those and be able to make
        # difference between peers having the same UID but different
        # addresses.
        self._peers = dict()
        # Populated from database in Peer.init. address to uid mapping
        self._addresses = dict()

        # ed25519 private key that includes the public key
        self._private_key = private_key

        # Hoply database should be a 4-tuple store aka. quad store
        # items are called as follow:
        #
        #   (collection, identifier, key, value)
        #
        # Unlike RDF vanilla naming scheme:
        #
        #   (graph, subject, predicate, object)
        #
        # Peer use five collections:
        #
        # - QADOM:BLACKLIST collection will store blacklisted ip (ip)
        #   the date the blackisting happened the first time
        #   (created-at) and the last time they were blacklisted
        #   (modified-at).  TODO: After sometime, remove ip from the
        #   blacklist.
        #
        # - QADOM:PEER collection store information about other
        #   peers. It is associated with a UID, an ip and an
        #   address. Because of NAT the port and ip can change and it
        #   is expected to happen. Bootstrap peers are marked with a
        #   bootstrap key.  TODO: keep track of peer scores.
        #
        # - QADOM:MAPPING collection will stores key-value pairs for
        #   use in Peer.get, Peer.get_at, Peer.set, Peer.store and
        #   Peer.value.  It is associated with timestamps created-at
        #   and modified-at.  That allows to forget old pairs.  key is
        #   always the hash of value.  TODO: keep modified at
        #   up-to-date.  TODO: remove old pairs.
        #
        # - QADOM:BAG collection associate a key with other keys. That
        #   is they should be in 2^UID_LENGTH space.  They can be
        #   freely set by peers.  It allows to link a well known key
        #   to other keys.  It is inspired from gnunet's filesystem
        #   keywords.  It is used by Peer.add, Peer.search, Peer.bag
        #   and Peer.bag_at.  TODO: score bag items and only send the
        #   most popular ones.
        #
        # - QADOM:NAMESPACE collection associate a public key with
        #   key-value pairs.  It is used by Peer.namespace_set,
        #   Peer.namespace_get, Peer.namespace and Peer.namespace_at.
        self._hoply = hoply

        # RPCProtocol set in Peer.listen
        self._protocol = None
        # Set in Peer.listen
        self._transport = None

        # run is a loop.run_in_executor bound to a particular
        # executor.  It is used to run database queries in other
        # threads.
        self._run = run

    def __repr__(self):
        return '<_Peer "%r">' % self._uid

    def close(self):
        self._transport.close()

    async def init(self, port, interface="127.0.0.1"):
        """Start listening on the given port and interface.

        """
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(
            RPCProtocol, local_addr=(interface, port)
        )
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

        # populate Peer._blacklist
        @h.transactional
        def blacklisted(tr):
            out = set(
                x["ip"]
                for x in tr.FROM("QADOM:BLACKLIST", h.var("uid"), "ip", h.var("ip"))
            )
            return out

        self._blacklist = await self._run(blacklisted, self._hoply)

        # populate Peer._peers and Peer._addresses
        @h.transactional
        def addresses(tr):
            query = h.compose(
                tr.FROM("QADOM:PEER", h.var("uid"), "ip", h.var("ip")),
                tr.where("QADOM:PEER", h.var("uid"), "port", h.var("port")),
                pick("ip", "port"),
            )
            return list(query)

        addresses = await self._run(addresses, self._hoply)
        await self._welcome_peers(addresses)

    async def blacklist(self, address):
        try:
            uid = self._addresses[address]
        except KeyError:
            pass
        else:
            del self._addresses[address]
            del self._peers[uid]
        self._blacklist.add(address[0])

        # store it
        @h.transactional
        def add(tr, ip):
            # TODO: Check blacklisting will not DoS the peer
            # TODO: This can blacklist the same ip multiple times
            now = int(time())
            uid = uuid4()
            tr.add("QADOM:BLACKLIST", uid, "ip", ip)
            tr.add("QADOM:BLACKLIST", uid, "created-at", now)
            tr.add("QADOM:BLACKLIST", uid, "modified-at", now)

        await self._run(add, self._hoply, address[0])

    async def bootstrap(self, address):
        """Add address to the list of peers.

        Send a ping to ADDRESS and add it with its uid as in the list of
        known peers.

        """
        log.debug("boostrap at %r", address)
        await self._welcome_peers([address])
        # TODO: make Peer._connect public and update the tests
        await self._connect()
        log.debug("bootstrap finished")

    async def _connect(self):
        # XXX: This is a tentative to populate the routing table with
        # enough nodes to cover 2^UID_LENGTH space and avoid lookup
        # KeyError because there is part of the space that self can
        # not reach!

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
        # XXX: MUST respect REPLICATION_MAX globally this is a hint
        # self returns False to the request if it is too far. This
        # MUST NOT happen except in cases where someone is using a
        # custom peer code. BUT it happens and it is a bug, see the
        # tests.
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
            return [
                random.randint(0, 2 ** UID_LENGTH) for x in range(self._replication)
            ]
        # XXX: The code is riddled with unpack/pack calls because Peer
        # stores key/uid as integer and msgpack doesn't accept such
        # big integers hence it is required to pass them as bytes.
        uid = unpack(uid)
        log.debug("[%r] find peers uid=%r from %r", self._uid, uid, address)
        # XXX: if this takes more than 5 seconds (see RPCProtocol) it
        # will timeout in the other side.
        uids = nearest(self._replication, self._peers.keys(), uid)
        out = [self._peers[x] for x in uids]
        return out

    # mapping procedures (vanilla dht api)

    async def value(self, address, key):
        """Remote procedure that returns the associated value or peers that
        are near KEY"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return (
                b"PEERS",
                [random.randint(0, 2 ** UID_LENGTH) for x in range(self._replication)],
            )

        log.debug("[%r] value key=%r from %r", self._uid, key, address)

        @h.transactional
        def out(tr, key):
            query = (
                x["value"]
                for x in tr.FROM("QADOM:MAPPING", key, "value", h.var("value"))
            )
            try:
                return next(query)
            except StopIteration:
                return None

        out = await self._run(out, self._hoply, unpack(key))
        if out is None:
            out = await self.peers((None, None), key)
            return (b"PEERS", out)
        else:
            return (b"VALUE", out)

    async def store(self, address, value):
        """Remote procedure that stores value locally with its digest as
        key"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return True

        log.debug("[%r] store from %r", self._uid, address)

        key = hash(value)
        ok = await self._is_near(key)
        if ok:
            # store it
            @h.transactional
            def add(tr, key, value):
                tr.add("QADOM:MAPPING", key, "value", value)

            await self._run(add, self._hoply, key, value)
            return True
        else:
            log.warning(
                "[%r] received a value that is too far, by %r", self._uid, address
            )
            return False

    # bag procedures

    async def add(self, address, key, value):
        """Remote procedure that adds VALUE to the list of uid at KEY"""
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return True

        log.debug("[%r] add key=%r value=%r from %r", self._uid, key, value, address)
        # TODO: unpack can fail in some occasion catch the correct exception
        key = unpack(key)
        value = unpack(value)
        if key > 2 ** UID_LENGTH or value > 2 ** UID_LENGTH:
            log.warning(
                "[%r] received a add that is invalid, from %r", self._uid, address
            )
            # XXX: pretend everything is ok
            return True

        ok = await self._is_near(key)
        if ok:
            # store it
            @h.transactional
            def add(tr, key, value):
                tr.add("QADOM:BAG", key, "value", value)

            await self._run(add, self._hoply, key, value)
            return True
        else:
            log.warning(
                "[%r] received a add that is too far, by %r", self._uid, address
            )
            return False

    async def search(self, address, key):
        """Remote procedure that returns values associated with KEY in the bag
        if any, otherwise return peers near KEY

        """
        log.debug("[%r] search uid=%r from %r", self._uid, key, address)
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return (
                b"PEERS",
                [random.randint(0, 2 ** UID_LENGTH) for x in range(self._replication)],
            )

        key = unpack(key)

        @h.transactional
        def out(tr, key):
            return list(
                x["value"] for x in tr.FROM("QADOM:BAG", key, "value", h.var("value"))
            )

        out = await self._run(out, self._hoply, key)

        if out:
            values = [pack(value) for value in out]
            return (b"VALUES", values)
        else:
            peers = await self.peers((None, None), pack(key))
            return (b"PEERS", peers)

    # namespace procedures

    async def namespace_set(self, address, public_key, key, value, signature):
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return True

        log.debug("namespace_set form %r", address)

        uid = hash(msgpack.packb((public_key, key)))
        ok = await self._is_near(uid)
        if ok:
            public = PublicKey.from_public_bytes(public_key)
            try:
                public.verify(signature, msgpack.packb((key, value)))
            except InvalidSignature:
                await self.blacklist(address)
                log.warning("[%r] invalid signature from %r", self._uid, address)
                # XXX: pretend everything is ok
                return True
            else:
                # store it
                @h.transactional
                def add(tr, public_key, key, value):
                    tr.add("QADOM:NAMESPACE", public_key, key, value)

                await self._run(add, self._hoply, public_key, unpack(key), value)
                return True
        else:
            log.warning(
                "[%r] received namespace_set that is too far, by %r", self._uid, address
            )
            return False

    async def namespace_get(self, address, public_key, key):
        if address[0] in self._blacklist:
            # XXX: pretend everything is ok
            return (
                b"PEERS",
                [random.randint(0, 2 ** UID_LENGTH) for x in range(self._replication)],
            )

        @h.transactional
        def out(tr, public_key, key):
            value = tr.FROM("QADOM:NAMESPACE", public_key, key, h.var("value"))
            try:
                return next(value)["value"]
            except StopIteration:
                return None

        out = await self._run(out, self._hoply, public_key, unpack(key))
        if out is None:
            # not found, return nearest peers
            uid = hash(msgpack.packb((public_key, key)))
            peers = await self.peers((None, None), pack(uid))
            return (b"PEERS", peers)
        else:
            return (b"VALUE", out)

    # helpers

    async def _welcome_peers(self, addresses):
        log.debug("[%r] welcome peers", self._uid)
        queries = dict()
        for address in addresses:
            if address[0] in self._blacklist:
                continue
            # TODO: only welcome unknown peers
            query = self._protocol.rpc(address, "ping", pack(self._uid))
            queries[address] = query
        responses = await gather(queries, return_exceptions=True)
        for (address, maybe_uid) in responses.items():
            if isinstance(maybe_uid, Exception):
                continue
            uid = unpack(maybe_uid)
            self._peers[uid] = address
            self._addresses[address] = uid

            # store it
            @h.transactional
            def maybe_add(tr, ip, port):
                query = h.compose(
                    tr.FROM("QADOM:PEER", h.var("uid"), "ip", ip),
                    tr.where("QADOM:PEER", h.var("uid"), "port", port),
                )
                try:
                    next(query)
                except StopIteration:
                    uid = uuid4()
                    now = int(time())
                    tr.add("QADOM:PEER", uid, "ip", ip)
                    tr.add("QADOM:PEER", uid, "port", port)
                    tr.add("QADOM:PEER", uid, "created-at", now)
                    tr.add("QADOM:PEER", uid, "modified-at", now)

            await self._run(maybe_add, self._hoply, *address)

    async def _reach(self, uid):
        log.debug("reach uid=%r", uid)
        try:
            return self._peers[uid]
        except KeyError:
            try:
                # try to reach node UID TODO: optimize to just do the
                # lookup, do not try to fetch values
                await self.get(uid)
            except KeyError:
                pass
            finally:
                return self._peers[uid]

    # local methods

    async def get_at(self, key, uid):
        """Get the value associated with KEY at peer with identifier UID"""
        log.debug("[%r] get_at key=%r uid=%r", self._uid, key, uid)
        try:
            peer = await self._reach(uid)
        except KeyError as exc:
            raise KeyError(key) from exc

        out = await self._protocol.rpc(peer, "value", pack(key))
        if out[0] == b"VALUE":
            value = out[1]
            if hash(value) == key:
                # store it
                @h.transactional
                def add(tr, key, value):
                    tr.add("QADOM:MAPPING", key, "value", value)

                await self._run(add, self._hoply, key, value)
                # at last!
                return value
            else:
                log.warning("[%r] received bad value from %r", peer)
                await self.blacklist(peer)
                return KeyError(key)
        else:
            raise KeyError(key)

    async def get(self, key):
        """Local method to fetch the value associated with KEY

        KEY must be an integer below 2^UID_LENGTH"""
        assert key <= 2 ** UID_LENGTH

        # check database
        @h.transactional
        def out(tr, key):
            query = (
                x["value"]
                for x in tr.FROM("QADOM:MAPPING", key, "value", h.var("value"))
            )
            try:
                return next(query)
            except StopIteration:
                return None

        out = await self._run(out, self._hoply, key)

        # proceed
        if out is None:
            out = await self._get(key)
            return out
        else:
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
                query = self._protocol.rpc(address, "value", uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b"VALUE":
                    value = response[1]
                    if hash(value) == unpack(uid):
                        # store it
                        @h.transactional
                        def add(tr, key, value):
                            tr.add("QADOM:MAPPING", key, "value", value)

                        await self._run(add, self._hoply, key, value)
                        # at last!
                        return value
                    else:
                        log.warning(
                            "[%r] bad value returned from %r", self._uid, address
                        )
                        await self.blacklist(address)
                        continue
                elif response[0] == b"PEERS":
                    await self._welcome_peers(response[1])
                else:
                    await self.blacklist(address)
                    log.warning(
                        "[%r] unknown response %r from %r",
                        self._uid,
                        response[0],
                        address,
                    )

    async def set(self, value):
        """Store VALUE in the network.

        Return the uid with which it is associated aka. sha256 integer representation."""
        # datagram max size minus "header", see RPCProtocol.
        if len(value) > (8192 - 28):
            raise ValueError("value too big")
        uid = pack(hash(value))
        # store it
        @h.transactional
        def add(tr, key, value):
            tr.add("QADOM:MAPPING", key, "value", value)

        await self._run(add, self._hoply, uid, value)
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
                queries = [
                    self._protocol.rpc(address, "store", value) for address in peers
                ]
                # XXX: Best effort. If the peers near uid are
                # malicious it will fail. Without a way to replicate
                # the value more often because legit peers will not
                # store a value that is NOT near, see Peer.set.
                await asyncio.gather(*queries, return_exceptions=True)
                return unpack(uid)
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, "peers", uid)
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

        response = await self._protocol.rpc(peer, "search", pack(key))
        if response[0] == b"VALUES":
            values = {unpack(x) for x in response[1]}
            # store it
            @h.transactional
            def add(tr, key, values):
                for value in values:
                    tr.add("QADOM:BAG", key, "value", value)

            await self._run(add, self._hoply, key, values)
            # at last!
            return values
        else:
            raise KeyError(key)

    async def _add(self, key, value):
        """Publish VALUE at KEY"""

        @h.transactional
        def add(tr, key, value):
            tr.add("QADOM:BAG", key, "value", value)

        await self._run(add, self._hoply, key, value)
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
                queries = [
                    self._protocol.rpc(address, "add", uid, value) for address in peers
                ]
                # XXX: Best effort.
                await asyncio.gather(*queries, return_exceptions=True)
                return
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, "peers", uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                await self._welcome_peers(response)

    async def _search(self, key):
        """Search values associated with KEY"""

        # init with database stored values
        @h.transactional
        def values(tr, key):
            return set(
                x["value"] for x in tr.FROM("QADOM:BAG", key, "value", h.var("value"))
            )

        values = await self._run(values, self._hoply, key)

        key = pack(key)
        queried = set()
        while True:
            # retrieve the k nearest peers and remove already queried peers
            peers = await self.peers((None, None), key)
            peers = [address for address in peers if address not in queried]
            # no more peer to query
            if not peers:
                # store it
                @h.transactional
                def add(tr, key, values):
                    for value in values:
                        tr.add("QADOM:BAG", key, "value", value)

                await self._run(add, self._hoply, key, values)
                # at last!
                return values
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, "search", key)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b"VALUES":
                    new = set([unpack(x) for x in response[1]])
                    values = values.union(new)
                elif response[0] == b"PEERS":
                    await self._welcome_peers(response[1])
                else:
                    await self.blacklist(address)
                    log.warning(
                        "[%r] unknown response %r from %r",
                        self._uid,
                        response[0],
                        address,
                    )

    # namespace local method

    async def namespace(self, key, value=None, public_key=None, signature=None):
        assert key <= 2 ** UID_LENGTH
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
        response = await self._protocol.rpc(peer, "namespace_get", public_key, key)
        if response[0] == b"VALUE":
            value = response[1]
            public_key_object = PublicKey.from_public_bytes(public_key)
            payload = msgpack.packb((key, value))
            try:
                public_key_object.verify(signature, payload)
            except InvalidSignature as exc:
                self.warning("invalid namespace set from %r", peer)
                await self.blacklist(peer)
                raise KeyError((public_key, unpack(key))) from exc
            else:
                # store it
                @h.transactional
                def add(tr, public_key, key, value):
                    tr.add("QADOM:NAMESPACE", public_key, key, value)

                await self._run(add, self._hoply, public_key, unpack(key), value)
                # at last!
                return value
        else:
            raise KeyError(public_key, unpack(key))

    async def _namespace_get(self, public_key, key, signature):
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
                # fallback to the database if present
                @h.transactional
                def value(tr, public_key, key):
                    value = tr.FROM("QADOM:NAMESPACE", public_key, key, h.var("value"))
                    try:
                        return next(value)["value"]
                    except StopIteration:
                        return None

                value = await self._run(value, self._hoply, public_key, unpack(key))
                if value is None:
                    # oops!
                    raise KeyError(public_key, unpack(key))
                else:
                    return value
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, "namespace_get", public_key, key)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                elif response[0] == b"VALUE":
                    value = response[1]
                    public_key_object = PublicKey.from_public_bytes(public_key)
                    payload = msgpack.packb((key, value))
                    try:
                        public_key_object.verify(signature, payload)
                    except InvalidSignature:
                        self.warning("invalid namespace get from %r", address)
                        await self.blacklist(address)
                        continue
                    else:
                        # store it
                        @h.transactional
                        def add(tr, public_key, key, value):
                            tr.add("QADOM:NAMESPACE", public_key, key, value)

                        await self._run(
                            add, self._hoply, public_key, unpack(key), value
                        )
                        # at last!
                        return value
                elif response[0] == b"PEERS":
                    await self._welcome_peers(response[1])
                else:
                    await self.blacklist(address)
                    log.warning(
                        "[%r] unknown response %r from %r",
                        self._uid,
                        response[0],
                        address,
                    )

    async def _namespace_set(self, key, value):
        """Publish VALUE at KEY"""
        # compute identifier of the node where to store that (public_key, key, value)
        public_key = self._private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        )
        # store it
        @h.transactional
        def add(tr, public_key, key, value):
            tr.add("QADOM:NAMESPACE", public_key, key, value)

        await self._run(add, self._hoply, public_key, key, value)
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
                        address, "namespace_set", public_key, key, value, signature
                    )
                    queries.append(query)
                # XXX: Best effort.
                await asyncio.gather(*queries, return_exceptions=True)
                return signature
            # query selected peers
            queries = dict()
            for address in peers:
                query = self._protocol.rpc(address, "peers", uid)
                queries[address] = query
            responses = await gather(queries, return_exceptions=True)
            for (address, response) in responses.items():
                queried.add(address)
                if isinstance(response, Exception):
                    continue
                await self._welcome_peers(response)


async def make_peer(uid, port, private_key=None, hoply=None, run=None):
    """Create a peer at PORT with UID as identifier"""
    if private_key is None:
        private_key = PrivateKey.generate()
    if hoply is None:
        cnx = MemoryConnexion("qadom")
        items = ("collection", "identifier", "key", "value")
        hoply = h.Hoply(cnx, "quads", items)
    if run is None:
        executor = ThreadPoolExecutor(4, "qadom:")
        loop = asyncio.get_event_loop()
        run = functools.partial(loop.run_in_executor, executor)
    peer = _Peer(uid, private_key, hoply, run)
    await peer.init(port)
    return peer
