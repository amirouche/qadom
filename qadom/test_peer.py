import logging

import daiquiri
import pytest
import msgpack

from cryptography.hazmat.primitives import serialization

from qadom import peer


daiquiri.setup(logging.DEBUG, outputs=('stderr',))


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
        peer = self.network.peers[address]
        proc = getattr(peer, name)
        out = await proc(self.peer._uid, *args)
        return out


class MockNetwork:

    def __init__(self):
        self.peers = dict()

    def add(self, peer):
        peer._protocol = MockProtocol(self, peer)
        self.peers[peer._uid] = peer



@pytest.mark.asyncio
async def test_bootstrap():
    # setup
    network = MockNetwork()
    one = make_peer()
    network.add(one)
    two = make_peer()
    network.add(two)

    # pre-check
    assert len(one._peers) == 0
    assert len(two._peers) == 0
    # exec
    await two.bootstrap(one._uid)
    # check
    assert two._peers == {one._uid: one._uid}
    assert one._peers == {two._uid: two._uid}



@pytest.mark.asyncio
async def test_dict():
    # setup
    value = b'test value'
    key = peer.digest(value)
    # make network and peers
    network = MockNetwork()
    one = make_peer()
    network.add(one)
    # use KEY as uid
    two = make_peer(key)
    network.add(two)
    three = make_peer()
    network.add(three)

    await two.bootstrap(one._uid)
    await three.bootstrap(one._uid)

    out = await three.set(value)
    assert out == key
    assert two._storage[key] == value

    # one is the only node not storing the value locally
    out = await one.get(key)
    assert out == value


@pytest.mark.asyncio
async def test_dict():
    # setup
    value = b'test value'
    key = peer.digest(value)
    # make network and peers
    network = MockNetwork()
    one = make_peer()
    network.add(one)
    # use KEY as uid
    two = make_peer(key)
    network.add(two)
    three = make_peer()
    network.add(three)

    await two.bootstrap(one._uid)
    await three.bootstrap(one._uid)

    out = await three.set(value)
    assert out == key
    assert two._storage[key] == value

    # one is the only node not storing the value locally
    out = await one.get(key)
    assert out == value


@pytest.mark.asyncio
async def test_bag():
    # setup
    network = MockNetwork()
    zero = make_peer()
    network.add(zero)
    one = make_peer()  # bootstrap node
    network.add(one)
    two = make_peer()
    network.add(two)
    three = make_peer()
    network.add(three)
    four = make_peer(4)
    network.add(four)

    await zero.bootstrap(one._uid)
    await two.bootstrap(one._uid)
    await three.bootstrap(one._uid)
    await four.bootstrap(one._uid)

    # exec
    await three.bag(4, 2006)
    await two.bag(4, 42)

    # check
    assert four._bag == {4: {42, 2006}}

    out = await one.bag(4)
    assert out == {42, 2006}
    out = await one.bag(4)
    assert out == {42, 2006}

    out = await zero.bag(4)
    assert out == {42, 2006}
    out = await zero.bag(4)
    assert out == {42, 2006}


@pytest.mark.asyncio
async def test_namespace():
    # setup
    network = MockNetwork()
    zero = make_peer()  # bootstrap node
    network.add(zero)
    one = make_peer()
    network.add(one)
    two = make_peer()
    network.add(two)
    three = make_peer()
    network.add(three)
    four = make_peer()
    network.add(four)

    await one.bootstrap(zero._uid)
    await two.bootstrap(zero._uid)
    await three.bootstrap(zero._uid)
    await four.bootstrap(zero._uid)

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

    for xxx in [zero, one, two, three, four]:
        out = await xxx.namespace(2006, public_key=public_key, signature=signature)
        assert out == b'echo alpha bravo'