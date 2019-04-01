# qadom

Experimental kademlia-inspired distributed hash table.

## TODO

- DHT
  - ``set(value)``
  - ``get(key)``

- keyword
  - ``append(keyword, key)``
  - ``query(keyword)``

- pubsub
  - ``publish(key, value)``
  - ``subscribe(key)``

- namespace
  - ``namespace_set(public_key, signature, key, value)``
	- where ``signature == sign(private_key, public_key, key, value)``
  - ``namespace_get(public_key, key)``
