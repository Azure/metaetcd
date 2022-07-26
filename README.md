# Meta Etcd

A proxy that enables sharding Kubernetes apiserver's keyspace across multiple etcd clusters.

## Why?

Etcd is typically disk IO constrained. Meaning that more/faster disks are required to scale past a certain point. If you don't have access to faster disks, the only answer is more of them. If you can't physically attach more storage to the host, the only answer is to add more hosts. But etcd is a single Raft consensus group in which every node in the cluster eventually receives every write operation. So adding more hosts doesn't increase throughput.

In order to balance etcd-associated disk IO across multiple hosts we need a "meta cluster" that is partitioned across multiple "member clusters".

Beyond scaling bottlenecks, partitioning is still useful - particularly for systems that colocate multiple etcd clusters on the same hardware. Sharding disk IO across more nodes naturally reduces the risk of noisy neighbors and allows nodes to run "hot" safely.

## Caveats

- 8 bytes of overhead per value stored
- Transactions can only reference a single key
- Create revision is not retained
- Raft cluster state is not returned in response headers
- Failed writes might increase watch latency
- Multi-key range queries fan out to all clusters
- Leases are only partially supported

## Architecture

### Clocking

In order to maintain a logic clock for the entire meta cluster, the proxy requires an additional "coordinator" cluster. A counter is incremented on this cluster for every write. The meta cluster's clock is also stored in one of the member clusters during transactions.

Since at least one member cluster always has the latest timestamp, the coordinator cluster doesn't need to be durable — it can use tmpfs. So it is unlikely to become a scaling bottleneck. If the coordinator cluster state is lost, the proxy will reconstitute it from the member clusters.

### Watches

The proxy watches the entire keyspace of every member cluster, buffers n messages, and replays them to clients. It's possible that messages will be received out of order, since network latency may vary between member clusters. In this case, it will buffer the out of order message until a timeout window is exceeded or the previous message has been received.

### Repartitioning

Currently the proxy does not support repartitioning, although it is implemented such that it is possible in the future. The long term goal is to support dynamically adding/removing member clusters at runtime with little to no impact.

## Basic Usage

Required flags:

- `--ca-cert` certificate used to verify the identity of etcd clusters (and proxy clients)
- `--client-cert` certificate presented to etcd clusters
- `--client-cert-key` key of `--client-cert`
- `--coordinator` URL of the coordinator cluster
- `--members` comma-separated list of member cluster URLs

By default, the meta cluster's proxy will be served on localhost:2379.
Although the listen address and server certificate can be configured with flags.

Important metrics:

- `metaetcd_request_count`: incremented for each request (by method)
- `metaetcd_time_buffer_timeouts_count`: incremented when a watch event is considered to be lost
- `metaetcd_clock_reconstitution`: incremented when the coordinator's state is lost and the clock is reconstituted from member clusters

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
