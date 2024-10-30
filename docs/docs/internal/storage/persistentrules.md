---
sidebar_position: 0
---

# Persistent Storage Rules

This section describes the rules that must be upheld for persistent storage.

## Pages

1. Each page key should be managed exclusively by a single `IPersistentStorageSession`; cross-session usage is not allowed.
2. An `IPersistentStorageSession` must always be used in a single-threaded context; multi-threaded calls are not permitted.
3. Each page key should be written to persistent storage only once per checkpoint to minimize unnecessary network traffic.
4. After a write operation, a page must be immediately available for read operations and return the written value, ensuring no eventual consistency delays.

## Recovery

1. The system must always be able to restore to the exact state of the last checkpoint.