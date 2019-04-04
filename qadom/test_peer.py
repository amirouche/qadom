import pytest

from qadom import peer


def random_peer():
    out = peer._Peer(peer.make_uid(), peer.PrivateKey.generate())
    return out


class MockProtocol:

    def __init__(self, network, peer):
        self.network = network
        self.peer = peer

    async def rpc(self, address, name, *args):
        peer = self.network.peers[address]
        proc = getattr(peer, name)
        out = await proc(peer._uid, *args)
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
    one = random_peer()
    network.add(one)
    two = random_peer()
    network.add(two)

    # pre-check
    assert len(one._peers) == 0
    assert len(two._peers) == 0
    # exec
    await two.bootstrap(one._uid)
    # check
    assert len(one._peers) == 1
    assert len(two._peers) == 1
