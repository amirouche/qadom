# qadom

Experimental kademlia-inspired distributed hash table.

## Getting started

To get started, install the dependencies with the following command:

```shell
pip install --user pipenv
git clone https://github.com/amirouche/qadom
cd qadom
pipenv install --dev
pipenv shell
```

Then run the following:

```shell
ipython -i peer0.py
```

Inside the `ipython` REPL run the following:

```python
asyncio.run_forever()
```

In another terminal run the following commands:

```python
pipenv shell
ipython -i peer1.py
```

That should print in the `ipython` REPL a big-ish integer:

```
2019-04-01 19:04:42,595 [30492] DEBUG    qadom.rpcudp: received datagram from ('127.0.0.1', 9997)
2019-04-01 19:04:42,595 [30492] DEBUG    qadom.rpcudp: received response True for message uid b'\xa9\x13&]\xecBBY\x91-\x08\xda^\xe3\xe5H' from ('127.0.0.1', 9997)


   44523492095186523269265158000087001266127815262863473237438729878159874507476

```

In that REPL run the following:

```python
loop.run_forever()
```

Last but not least, the following will simulate a new peer joining the
network, run the following in a new terminal:


```shell
pipenv shell
ipython -i peer2.py
```

That's all folks!

## Documentation

Read the code it has docstrings!!!

### Local API

#### `peer.get(key)`

Takes an integer below 2^256 as `KEY` and return the value associated
with it in the network. If there is no value is associated with `KEY`
raises `KeyError`.

#### `peer.set(value)`

Takes bytes as `VALUE` that must be smaller that 8K, store it locally
and somewhere in the network. Returns the key where it is stored.

#### `peer.bag(key)`

Takes an integer below 2^256 as `KEY` and return the set of
identifiers associated with it in the network.

#### `peer.bag(key, value)`

Both `KEY` and `VALUE` must be integers below 2^256. `VALUE` will be
associated with `KEY` in the network.

Use `peer.bag(key)` to fetch the set of values associated with `KEY`
including `VALUE`.

#### `peer.namespace(key, value)`

`KEY` must be an integer below 2^256 and value must be bytes less
than 8K. Store KEY and VALUE under the namespace described by the
peer's public key.

Use `peer.namespace(key, public_key=public_key, signature=signature)`
to retrieve `VALUE`.

#### `peer.namespace(key, public_key=public_key, signature=signature)`

`KEY` and `PUBLIC_KEY` and `SIGNATURE` must be integers. `KEY` must be
below 2^256.

Returns the value associated with `PUBLIC_KEY` and `KEY` that match
`SIGNATURE`.


### RPC API

- dict

  - `peer.set(value)`
  - `peer.get(key)`

- bag

  - `peer.add(key, value)`
  - `peer.search(key)`

- pubsub (TODO)

  - `peer.publish(key, value)`
  - `peer.subscribe(key)`

- namespace

  - `peer.namespace_set(public_key, key, value, signature)`
  - `peer.namespace_get(public_key, key)`
