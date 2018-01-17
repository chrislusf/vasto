# Vasto

A distributed high-performance key-value store. On Disk. Eventual consistent. HA. Able to grow or shrink without service interruption.

Vasto scales embedded [RocksDB](https://github.com/facebook/rocksdb) key-value store into a distributed key-value store,
adding sharding, replication, and support operations to
1. create a new keyspace
1. delele an existing keyspace
1. grow a keyspace
1. shrink a keyspace
1. replace a node in a keyspace

# Why
A key-value store is often re-invented. Why there is another one?

Vasto enables developers to setup a distributed key-value store as simple as creating a map object.

The operations, such as creating/deleting the store, connection configurations, partitioning, replications, 
seamlessly adding/removing servers, are all managed automatically.

In a sense, Vasto is an in-house cloud providing distributed key-value stores as a service, 
minus the need to balance performance and cloud service costs.

# Architecture

There are one Vasto master and N number of Vasto stores, plus Vasto clients or Vasto proxies/gateways.
1. The Vasto stores are basically simple wrapper of RocksDB. 
1. The Vasto master manage all the Vasto stores and Vasto clients.
1. Vasto clients rely on master to connect to the right Vasto stores.
1. Vasto gateways use Vasto client libraries to support different APIs.

The master is the brain of the system. Vasto does not use gossip protocols, or other consensus algorithms.
Vasto uses a single master for simple setup, fast failure detection, fast topology changes, and precise coordinations.
The master only contains soft states and is only required when topology changes. 
So even if it ever crashes, a simple restart will recover everything.

The Vasto stores simply pass get/put/delete/scan requests to RocksDB. 
One Vasto store can host multiple db instances.

The Vasto gateway uses the client library and talk to the stores directly. It is a reverse proxy, and can be installed
on any application machine to reduce one network hop. Or can be on its dedicated machine to reduce number of connections
to the Vasto stores if both the number of stores and the number of clients are very high.


# Life cycle

One Vasto cluster has one master and multiple Vasto stores. When the store joins the cluster, it is just empty.

When the master receives a request to create a keyspace with x shards and replication factor = y, the master would
1. find x stores that meet the requirement and assign it to the keyspace
1. ask the stores to create the shards, including replicas.
1. inform the clients of the store locations

When the master receives a request to resize the keyspace from m shards to n shards, the master would
1. if size increased, find n-m stores that meet the requirement and assign it to the keyspace
1. ask the stores to create the shards, including replicas.
1. prepare the data to the new stores
1. direct the clients traffic to the new stores
1. remove retiring shards

# Hashing algorithm

To achive minimum data movement and avoid overloading a few particular existing stores, 
Vasto used jumping hash to allocate data.

# APIs

Work in progress.

Currently only basic go library is provided. The gateway is not ready yet.
