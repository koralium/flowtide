---
sidebar_position: 6
---

# Iteration Operator

The *iteration operator* allows a user to do to iterative streams, such as a recursive join in a tree structure.
The operator is not defined as a standard operator in substrait. So this uses custom relations.

It is defined by two different relations:

* IterationRelation
* IterationReferenceReadRelation

## Iteration Relation

The iteration relation uses ExtensionMultiRel in substrait and is defined as follows in protobuf:

```
message IterationRelation {
    string iterationName = 1;
}
```

The first input in ExtensionMultiRel is the loop relation, the second input is input to the iteration itself.
The second input is optional.

IterationName exist since there can be multiple nested iterations.

## Iteration Reference Read Relation

This relation is used to tell where the data from the iteration operator should be sent to inside of the loop.
This relation should only be used inside the loop relation.

It is defined by ExtensionLeafRel and has the following message:

```
message IterationReferenceReadRelation {
    // Name of the iteration to get data from
    string iterationName = 1;
}
```

## Implementation

The iteration operator differs a bit from the other operators in how it does checkpointing.
To make sure a checkpoint contains all processed data before comitting to a checkpoint it follows these steps:

1. If there is no input to the operator, a dummy read operator is created that only sends checkpoint events.
2. On checkpoint send a LockingEventPrepare to the loop.
3. All operators in loop adds information if they have another dependency that is not yet in checkpoint.
4. If any message was recieved before the iteration operator recieves the LockingEventPrepare message, or a dependency is not in checkpoint, the message is resent.
5. When all conditions above are met, the checkpoint is sent throught the loop.
6. When the operator recieves the checkpoint from the loop, it first sends out watermark information, and then the checkpoint to the rest of the stream.

## Metrics

The *Iteration Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |

:::info

At this point, an iteration operator will never be unhealthy.
If there is a failure against the state, the stream will instead restart.

:::