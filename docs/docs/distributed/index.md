---
sidebar_position: 1
---

# Distributed Mode

> [!WARNING]
> Distributed mode is still experimental.

Distributed mode splits one stream into multiple *substreams*. Each substream is a complete Flowtide stream with its own state storage, and they can run in different processes or on different machines. The substreams exchange data with each other.

This is different from [Parallelism](../parallelism.md), which partitions operators inside a single stream. Parallelism helps when one operator is a bottleneck on one machine. Distributed mode helps when the whole stream no longer fits on one machine, either in memory, state size or compute.

## How it works

The plan is split so operators such as joins, aggregates and window functions run with one partition copy in every substream. Data is scattered between the substreams by hashing on the partition keys, and the results are gathered back to the substream that runs the sink.

Operators that keep the same partition key stay together. For instance a join followed by an aggregate grouped on the join key runs the whole pipeline in every substream, only the final results cross between substreams.

Watermarks and checkpoints flow between the substreams together with the data, so they work the same way as in a single stream.

## Consistency and failure recovery

Substreams checkpoint together. A checkpoint only completes when the substreams it exchanges data with have acknowledged it, so no substream can throw away state that another substream might still need.

If a substream fails, all substreams it exchanges data with roll back to a common checkpoint, and the data in between is replayed from the sources. This is the same recovery model as a single stream, sources must be able to resend data from their last committed position.

No substream starts a new checkpoint before all substreams it exchanges data with have committed the previous one, so connected substreams are never more than one checkpoint apart. When substreams start they compare their restored checkpoint versions, if they differ all of them recover to the lower one and catch up by replay.

### Committing to external systems

A checkpoint that is durable in one substream can still be rolled back by one version, when a connected substream fails before the checkpoint became durable there as well. A sink that writes to a system that can not be rolled back must therefore not treat a completed checkpoint as final in distributed mode.

Sinks get a separate call for this, *CommitVersion*. It is called with a version once every connected substream, direct or through other substreams, is known to be durable at it. From that point none of them restores below that version. Substreams that exchange no data with each other, not even through others, do not wait for each other. It is also called when a stream starts, with the version it restored. In a stream that is not distributed it is called directly after each checkpoint.

It can be called again with a version it was already called with, for instance at the start after a restart, so committing a version must be idempotent. A substream that stops cleanly before the others are known to be durable at its last version does not commit it, the next start does.

### State storage must survive

A substream never rolls back below a version that was handed to *CommitVersion* in a substream it is connected to, directly or through other substreams. A substream that comes back without its state storage asks the others to start over from the beginning, which they refuse: they log an error and the stream stops making progress until all substreams are restarted from empty state.

State storage that survives the process, and in a cluster is reachable from every machine a substream can run on, is a requirement in distributed mode.

## Hosting options

* [In-process hosting](inprocess.md) runs all substreams in one process. Mainly useful for testing and to verify how a plan distributes.
* [Orleans hosting](orleans.md) runs each substream as a grain in an Orleans cluster, with automatic placement and recovery when silos fail.

## Controlling the distribution

* [Automatic distribution](automaticdistribution.md) takes a completely normal plan and splits it into a chosen number of substreams.
* [SQL substream statements](sqlsubstreams.md) give manual control over which statements run in which substream.

## Limitations

* Recursive queries are never split, the whole pipeline that contains one stays in one substream.
* Joins without any equality condition can not be partitioned and stay in one substream.
* Aggregates that use the surrogate key function are not distributed.
* A distributed view consumed from another substream must declare *SCATTER_BY*, broadcast between substreams is not supported.
* On a single machine distributed mode is normally slower than running the plan as a single stream. Data between substreams on the same machine is passed by reference, serialization only happens when the data crosses to another machine, but the exchange still adds partitioning and queueing. Distributed mode pays off when the stream needs more resources than one machine has.
