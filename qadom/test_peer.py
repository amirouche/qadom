import asyncio
import logging
import random
import os

from time import time

import daiquiri
import json
import networkx as nx
import pytest
import uvloop

from cryptography.hazmat.primitives import serialization

import hoply as h
from hoply.memory import MemoryConnexion
from qadom import peer


daiquiri.setup(logging.INFO, outputs=('stderr',))
log = logging.getLogger(__name__)


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# randomize tests but make it predictable
SEED = os.environ.get('QADOM_SEED')
if SEED is None:
    PEER_COUNT_MAX = 10**1
    SEED = {
        'RANDOM_SEED': random.getrandbits(2048),
        'PEER_COUNT': random.randint(PEER_COUNT_MAX // 2 - 1, PEER_COUNT_MAX),
    }
else:
    with open(SEED) as f:
        SEED = json.load(f)


RANDOM_SEED = SEED['RANDOM_SEED']
random.seed(RANDOM_SEED)
PEER_COUNT = SEED['PEER_COUNT']

with open('SEED.json', 'w') as f:
    json.dump(SEED, f)


async def mock_run(proc, *args):
    return proc(*args)


def make_peer(uid=None):
    uid = uid if uid is not None else peer.make_uid()
    private_key = peer.PrivateKey.generate()
    cnx = MemoryConnexion('qadom')
    items = ('collection', 'identifier', 'key', 'value')
    hoply = h.Hoply(cnx, 'quads', items)
    out = peer._Peer(uid, private_key, hoply, mock_run, replication=3)
    return out


class MockProtocol:

    def __init__(self, network, peer):
        self.network = network
        self.peer = peer

    async def rpc(self, address, name, *args):
        peer = self.network.peers[address[0]]
        proc = getattr(peer, name)
        start = time()
        out = await proc((self.peer._uid, None), *args)
        delta = time() - start
        assert delta < 5, "RPCProtocol allows 5s delay only"
        return out


class MockNetwork:

    def __init__(self):
        self.peers = dict()

    def add(self, peer):
        peer._protocol = MockProtocol(self, peer)
        self.peers[peer._uid] = peer

    def choice(self):
        return random.choice(list(self.peers.values()))


async def make_social_network():
    log.info("network size=%r", PEER_COUNT)
    m = 1 + PEER_COUNT % 5
    m = m if 1 < m < PEER_COUNT else PEER_COUNT - 1
    graph = nx.barabasi_albert_graph(PEER_COUNT, m, RANDOM_SEED)
    network = MockNetwork()
    peers = dict()
    for node in graph.nodes:
        peer = make_peer()
        network.add(peer)
        peers[node] = peer
    for node, peer in peers.items():
        for neighbor in graph.neighbors(node):
            neighbor = peers[neighbor]
            await peer.bootstrap((peer._uid, None))

    # run connect, this simulate the peers connecting to an existing
    # network.
    for peer in network.peers.values():
        await peer.connect()

    return network


cached_social_network = None


async def random_social_network():
    global cached_social_network
    if cached_social_network is None:
        cached_social_network = await make_social_network()
    return cached_social_network


cached_complete_network = None


async def complete_network():
    global cached_complete_network
    if cached_complete_network is None:
        log.info("network size=%r", PEER_COUNT)
        graph = nx.complete_graph(PEER_COUNT)
        network = MockNetwork()
        peers = dict()
        for node in graph.nodes:
            peer = make_peer()
            network.add(peer)
            peers[node] = peer
        for node, peer in peers.items():
            for neighbor in graph.neighbors(node):
                neighbor = peers[neighbor]
                await peer.bootstrap((peer._uid, None))

        # run connect, this simulate the peers connecting to an
        # existing network.
        for peer in network.peers.values():
            await peer.connect()

        cached_complete_network = network

    return cached_complete_network


async def simple_network():
    network = MockNetwork()
    for i in range(5):
        peer = make_peer()
        network.add(peer)
    bootstrap = peer
    for peer in network.peers.values():
        await peer.bootstrap((bootstrap._uid, None))
    for peer in network.peers.values():
        await peer.bootstrap((bootstrap._uid, None))

    # run connect, this simulate the peers connecting to an existing
    # network.
    for peer in network.peers.values():
        await peer.connect()

    return network


NETWORKS = os.environ.get('QADOM_NETWORKS')
if NETWORKS:
    NETWORK_MAKERS = list()
    NETWORKS = NETWORKS.split(',')
    if 'SIMPLE' in NETWORKS:
        NETWORK_MAKERS.append(simple_network)
    if 'SOCIAL' in NETWORKS:
        NETWORK_MAKERS.append(random_social_network)
    if 'COMPLETE' in NETWORKS:
        NETWORK_MAKERS.append(complete_network)
else:
    NETWORK_MAKERS = [random_social_network, complete_network, simple_network]


@pytest.mark.parametrize("make_network", NETWORK_MAKERS)
@pytest.mark.asyncio
async def test_bootstrap(make_network):
    network = await make_network()

    # setup
    one = network.choice()
    two = network.choice()

    # exec
    await two.bootstrap((one._uid, None))

    # check
    assert two._peers
    assert one._peers


@pytest.mark.parametrize("make_network", NETWORK_MAKERS)
@pytest.mark.asyncio
async def test_dict(make_network):
    network = await make_network()
    # setup
    value = b'test value'
    key = peer.hash(value)
    # make network and peers
    one = network.choice()
    two = network.choice()
    three = network.choice()
    four = network.choice()

    # exec
    out = await three.set(value)

    # check
    assert out == key

    fallback = list()
    for xxx in (one, two, three, four):
        try:
            out = await xxx.get(key)
        except KeyError:
            fallback.append(xxx)
        else:
            assert out == value

    for xxx in fallback:
        log.warning('fallback for peer %r', xxx)
        out = await xxx.get_at(key, three._uid)
        assert out == value


@pytest.mark.parametrize("make_network", NETWORK_MAKERS)
@pytest.mark.asyncio
async def test_bag(make_network):
    network = await make_network()
    # setup
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
        assert list(out.values()) == [{42, 2006}] * len(out)


@pytest.mark.parametrize("make_network", NETWORK_MAKERS)
@pytest.mark.asyncio
async def test_namespace(make_network):
    network = await make_network()

    # setup
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
