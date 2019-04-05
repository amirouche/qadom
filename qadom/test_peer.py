import asyncio
import logging
import random

import daiquiri
import pytest
import msgpack

from cryptography.hazmat.primitives import serialization

from qadom import peer


daiquiri.setup(logging.DEBUG, outputs=('stderr',))

# randomize test but make it reproducible
SEED = random.randint(0, 2**8192)
SEED = 907945071347060478895347860610091774107538839369523630931316209918974199389380620980446539221113643346986187075705375836794833992979728587415383366100053033376611520835622222672857441960549198674094134896271646535233023097907313131995755713780848338087544177584729229413885795810192886025005958053950513550074950474100228842901346660834458056983541905385250919630572386134970856936425335236289684902535498038692428505496157948477811320823062576591747817054546289731241556318525564471079356032922219941177424189303690636393987904619914223433512096331096156460483893957341408975667509439427055592684416707698376914896226514055203485179219062506542140974840786677373574679630997001566923258642842845034205580563361292049841831326546682628777444270814186285721267714724398515735181733300080216773829919725855655382368179917666716069773576818394964318333700372823280264748072454094990308681497779874601262769671456758704943348000682557738078464269601404493381859630276042938077946528401128597515329339654459674269560672300300196416377777299377725437082681950883402671696547327086615774674271607035908373071483677741151453726253071997190009448963249644994223280960784991878604460583168904782749832484807563294838265564086065465625114940382421502006724056542634767221971450742599665356871375588376754244646903479780270550692642630337103533212246151837082574476424230197175648738728182080408899233255042391666134277246970775101143851258296245111564235727743399049537523502127443508160251105109212746248731499909740527495281454782239821099807090503903293530912303602638776915657394786761563509886159184712179964862941749949796167311040142880890255398327615998635615648329510779960848314460557566452749761553179178988035005390137335189159822765914064917067400414651137261148080981713693283524599395192599781186382466767436882081638612808464291108241539680597765968446955917036141945872639996304254914733589967557571826169798348213565560321126664647002305185723922964661403471964107147834733866514596042109039835203406798090639926026098158153837785114519463098636232073089828362322961601417004040803585528379761013782085169678296391057777189304951071232394963566154531657642045728808365332257504791188198945579869104451107366040804767846120715947852182078894179312465369526028950562877884035913536675425380832445896839964676503980587165973337628219769066599897653213347215255070063494015892506133742619119567700394173189544341253005403103981404795132507206402309571473289904603


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
    assert two._peers == {one._uid: (one._uid, None)}
    assert one._peers == {two._uid: (two._uid, None)}


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
    queries = []
    for xxx in (one, two, three, four):
        query = xxx.get(key)
        queries.append(query)
    out = await asyncio.gather(*queries, return_exceptions=True)
    assert out == [value] * 4


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
    await two.bag(4, 42)

    queries = []
    for xxx in [zero, one, two, three, four]:
        query = xxx.bag(4)
        queries.append(query)
    out = await asyncio.gather(*queries, return_exceptions=True)
    assert out == [{42, 2006}] * 5



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

    queries = []
    for xxx in [zero, one, two, three, four]:
        query = xxx.namespace(2006, public_key=public_key, signature=signature)
        queries.append(query)
    out = await asyncio.gather(*queries, return_exceptions=True)
    assert out == [b'echo alpha bravo'] * 5
