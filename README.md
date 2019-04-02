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

That all folks!

## Documentation

Read the code it has docstrings!!

### Local API

#### `peer.get(key)`

Takes an integer below 2^256 as `KEY` and return the value associated
with it in the network. If there is no value is associated with `KEY`
raises `KeyError`.

#### `peer.set(value)`

Takes bytes as `VALUE` that must be smaller that 8K, store it locally
and somewhere in the network. Returns the key where it is stored.

### `peer.key(key)`

Takes an integer below 2^256 as `KEY` and return the identifiers
associated with it in the network.

### `peer.key(key, value)`

Both `KEY` and `VALUE` must be integers below 2^256. `VALUE` will be
associated with `KEY` in the network. Later, use `peer.key(key)` to
fetch the values associated with `KEY` including `VALUE`.

### RPC API

- DHT
  - `peer.set(value)`
  - `peer.get(key)`

- keys
  - `peer.append(key, value)`
  - `peer.search(key)`

- pubsub (TODO)
  - `peer.publish(key, value)`
  - `peer.subscribe(key)`

- namespace (TODO, see https://stackoverflow.com/q/55455548/140837)
  - `peer.namespace_set(public_key, signature, key, value)`
	- where `signature == sign(private_key, public_key, key, value)`
  - `peer.namespace_get(public_key, key)`
