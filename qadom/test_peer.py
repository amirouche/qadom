import asyncio
import logging
import random

import daiquiri
import pytest
import msgpack

from cryptography.hazmat.primitives import serialization

from qadom import peer


daiquiri.setup(logging.DEBUG, outputs=('stderr',))
log = logging.getLogger(__name__)


# randomize test but make it reproducible
SEED = random.randint(0, 2**8192)


with open('SEED.txt', 'w') as f:
    f.write(str(SEED))

random.seed(SEED)


# choosen so that tests don't run for too long
PEER_COUNT_MAX = 10**5


def make_peer(uid=None):
    uid = uid if uid is not None else peer.make_uid()
    private_key = peer.PrivateKey.generate()
    out = peer._Peer(uid, private_key, 1)
    return out


class MockProtocol:

    def __init__(self, network, peer):
        self.network = network
        self.peer = peer

    async def rpc(self, address, name, *args):
        peer = self.network.peers[address[0]]
        proc = getattr(peer, name)
        out = await proc((self.peer._uid, None), *args)
        return out


class MockNetwork:

    def __init__(self):
        self.peers = dict()

    def add(self, peer):
        peer._protocol = MockProtocol(self, peer)
        self.peers[peer._uid] = peer

    def choice(self):
        return random.choice(list(self.peers.values()))


def make_network():
    network = MockNetwork()
    count = random.randint(PEER_COUNT_MAX // 3, PEER_COUNT_MAX)
    for i in range(count):
        peer = make_peer()
        network.add(peer)
    return network


@pytest.mark.asyncio
async def test_bootstrap():
    # setup
    network = make_network()
    one = network.choice()
    two = network.choice()

    # pre-check
    assert len(one._peers) == 0
    assert len(two._peers) == 0

    # exec
    await two.bootstrap((one._uid, None))

    # check
    assert two._peers
    assert one._peers


@pytest.mark.asyncio
async def test_dict():
    # setup
    value = b'test value'
    key = peer.hash(value)
    # make network and peers
    network = make_network()
    one = network.choice()
    two = network.choice()
    three = network.choice()
    four = network.choice()

    await two.bootstrap((one._uid, None))
    await three.bootstrap((one._uid, None))
    await four.bootstrap((one._uid, None))

    # exec
    out = await three.set(value)

    # check
    assert out == key
    queries = []
    for xxx in (one, two, three, four):
        query = xxx.get(key)
        queries.append(query)
    out = await asyncio.gather(*queries, return_exceptions=True)
    assert out == [value, value, value, value]


@pytest.mark.asyncio
async def test_bootstrap_node_doesnt_know_everybody():
    # setup
    value = b'test value'
    key = peer.hash(value)
    # make network and peers
    network = make_network()
    one = network.choice()
    two = network.choice()
    three = network.choice()
    four = network.choice()

    await two.bootstrap((one._uid, None))
    await three.bootstrap((one._uid, None))
    await four.bootstrap((three._uid, None))

    # exec
    out = await three.set(value)

    # check
    assert out == key
    queries = dict()
    for xxx in (one, two, three, four):
        query = xxx.get(key)
        queries[xxx] = query

    canonical = await peer.gather(queries, return_exceptions=True)
    fallback = {}
    for xxx, response in canonical.items():
        if isinstance(response, Exception):
            fallback[xxx] = xxx.get_at(key, three._uid)
    if fallback:
        log.warning('fallback')
        out = await peer.gather(fallback, return_exceptions=True)
        assert list(out.values()) == [value] * len(out)


@pytest.mark.asyncio
async def test_bag():
    # setup
    network = make_network()
    zero = network.choice()
    one = network.choice()  # bootstrap node
    two = network.choice()
    three = network.choice()
    four = network.choice()

    await zero.bootstrap((one._uid, None))
    await two.bootstrap((one._uid, None))
    await three.bootstrap((one._uid, None))
    await four.bootstrap((one._uid, None))

    # exec
    await three.bag(4, 2006)
    await three.bag(4, 42)

    queries = dict()
    for xxx in [zero, one, two, three, four]:
        query = xxx.bag(4)
        queries[xxx] = query
    canonical = await peer.gather(queries, return_exceptions=True)
    fallback = dict()
    for xxx, response in canonical.items():
        if isinstance(response, Exception):
            query = xxx.bag_at(4, three._uid)
            fallback[xxx] = query
    if fallback:
        log.warning('fallback')
        out = await peer.gather(fallback, return_exceptions=True)
        assert list(out.values()) == [{4, 2006}] * len(out)


@pytest.mark.asyncio
async def test_namespace():
    # setup
    network = make_network()
    zero = network.choice()  # bootstrap node
    one = network.choice()
    two = network.choice()
    three = network.choice()
    four = network.choice()

    await one.bootstrap((zero._uid, None))
    await two.bootstrap((zero._uid, None))
    await three.bootstrap((zero._uid, None))
    await four.bootstrap((zero._uid, None))

    # exec
    key = 2006
    value = b'echo alpha bravo'
    signature = await four.namespace(key, value)

    # check
    public_key = four._private_key.public_key()
    public_key = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    )

    queries = dict()
    for xxx in [zero, one, two, three, four]:
        query = xxx.namespace(2006, public_key=public_key, signature=signature)
        queries[xxx] = query
    canonical = await peer.gather(queries, return_exceptions=True)
    fallback = dict()
    for xxx, response in canonical.items():
        if isinstance(response, Exception):
            query = xxx.namespace_at(2006, public_key, signature, four._uid)
            fallback[xxx] = query
    if fallback:
        log.warning('fallback')
        out = await peer.gather(fallback, return_exceptions=True)
        assert list(out.values()) == [b'echo alpha bravo'] * len(out)
