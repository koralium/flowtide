---
sidebar_position: 0
---

# Persistent Storage Rules

This section describes the rules that must be upheld for persistent storage.

## Pages

1. Each page key should be managed exclusively by a single `IPersistentStorageSession`; cross-session usage is not allowed.
2. An `IPersistentStorageSession` must always be used in a single-threaded context; multi-threaded calls are not permitted.
3. Each page key should be written to persistent storage only once per checkpoint to minimize unnecessary network traffic.
4. After a Commit operation, all page must be immediately available for read operations and return the written values, ensuring no eventual consistency delays.

## Recovery

1. The system must always be able to restore to the exact state of the last checkpoint.
2. Any pages written must be restored to their previous value when recovering to the previous checkpoint.
3. Deleted pages must be restored with their previous value when recovering to the previous checkpoint.
4. Recovery only needs to support the most recent successful checkpoint; there is no requirement to support rollbacks to earlier checkpoints. 