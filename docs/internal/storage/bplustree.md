---
sidebar_position: 1
---

# B+ Tree

The B+ tree is used in almost all operators that require state management of rows, for instance in a join which must keep track of rows from the left input and right input.

A B+ tree can be created in an operator by calling `GetOrCreateTree` from the `IStateManagerClient` during `InitializeOrRestore`.

Example:

```csharp
protected override async Task InitializeOrRestore(JoinState? state, IStateManagerClient stateManagerClient)
{
    _leftTree = await stateManagerClient.GetOrCreateTree("left",
        new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
        {
            Comparer = _leftInsertComparer,
            KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Left.OutputLength, MemoryAllocator),
            ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
            UseByteBasedPageSizes = true,
            MemoryAllocator = MemoryAllocator
        });
}
```

The tree requires four generic parameters:

1. The key type, in the example above it is `ColumnRowReference`.
2. Value type, in the example above `JoinWeights`.
3. The storage solution for the keys, this allows to optimize the storage of the keys.
4. The storage solution for the values, this allows to optimize the storage of the values.

## Upsert

Upsert is used to insert or update data.

Example:

```csharp
// if the tree has int as key, and string as value
await tree.Upsert(1, "Hello");
```

## Delete

Deletes the data for a key.

Example:

```csharp
await tree.Delete(1);
```

## Read-Modify-Write (RMW)

Allows reading and then modifiying the data, this can result in a 'none', 'upsert' or 'delete' operation.

Example:

```csharp
await tree.RMW(1, "hello", (inputValue, currentValue, found) => {
    if (found && inputValue == null) {
        return (default, GenericWriteOperation.Delete);
    }
    return (inputValue, GenericWriteOperation.Upsert);
});
```

## Get Value

Returns the value for a key.

Example:

```csharp
var (found, value) = await tree.GetValue(1);
```

## Iterating over the values

Since this is a B+ tree, one of the main uses is to iterate over the values in the tree.
This is done with the `CreateIterator` method.

```csharp
var iterator = tree.CreateIterator();
```

There are three methods on the iterator, `SeekFirst` which finds the most left value, `Seek` locates the position of a key, and `Reset` which resets the iterator.

Full example:

```csharp
var iterator = tree.CreateIterator();
await iterator.Seek(3);

// Iterate over each page, this is async since it might fetch data from persistent storage.
await foreach(var page in iterator) {
    // Iterate over the key values in that page
    foreach (var keyValuePair in page) {
        
    }
}
```

## Commit

When data has been written, it is not yet persisted. To persist the data one must call `Commit`.
This is done in the `OnCheckpoint` method in an operator.
But if the tree is used to store temporary data, `Commit` should not be called.
A long-lived tree that is never committed should be an [ephemeral tree](#ephemeral-trees) instead, so its deleted pages leave no bookkeeping behind.

Example:

```csharp
public override async Task<OperatorState> OnCheckpoint()
{
    await _tree.Commit();
    return new OperatorState();
}
```

## Ephemeral trees

An ephemeral tree is a B+ tree that is never persisted. It is created with `GetOrCreateEphemeralTree` from the `IStateManagerClient`, with the same options as `GetOrCreateTree`:

```csharp
_catalog = await stateManagerClient.GetOrCreateEphemeralTree("catalog", options);
```

An ordinary tree that is never committed keeps a record of every page it ever deleted, waiting for a commit that never comes. An ephemeral tree forgets a deleted page at once, so its bookkeeping follows its live pages.

### Who may use it

A tree may be ephemeral only if one of these holds:

* it is empty at every checkpoint, or
* after a restart or recovery it is rebuilt in full, from replay or from an external source of truth.

Its contents never reach a checkpoint. The temporary trees of `BulkAggregateOperator` and `BulkWindowOperator` are counterexamples: a checkpoint can arrive while they still hold groups waiting for a watermark, so they are committed, and as ephemeral trees they would lose those groups in a crash. A tree that adopts this mode should come with a crash-and-recover test of its own.

### Lifecycle

| Event | State of the tree afterwards |
|---|---|
| `Commit()`, then a checkpoint | Unchanged in memory and in the spill. Nothing is written for the tree; the manager may still checkpoint the binding of its name |
| Reopening the name in the same process | The same client, with the same contents |
| `Clear()` | Empty |
| Recovery, including after another client's failed commit | Empty once reopened |
| Stop and start, or a process restart | Empty |
| Another client's commit fails | The checkpoint fails as it does today, the ephemeral tree does not consume or clear the failure |
| Opening a name that has persisted contents | Throws `InvalidOperationException` |
| Opening a name that is open with the other lifecycle, through any factory | Throws `InvalidOperationException` |
| Creation fails after the name is registered | The registration stays, recovery resets it, and the retry reuses it |

An iterator does not survive a `Clear()` or a recovery.

The serializer's `InitializeAsync` runs when the tree is created, its `CheckpointAsync` never runs.

`StateExists` reports a registration, not durable contents. It is true once the name is registered in the current manager. After a restart it is true only when the restored manager checkpoint holds the binding of the name, and the tree still starts empty.

Use a new name when a tree changes lifecycle. A name that held persisted contents cannot be opened as an ephemeral tree. After a restart, a name that was only ever ephemeral can be opened as an ordinary tree, since nothing was persisted for it.

### Spill and memory

An ephemeral tree uses the same cache, eviction and spill as an ordinary tree. A live page that is evicted is written to the temporary file cache and read back on demand. A deleted page is removed from the cache and its spill is freed in the same call.

What is owned follows the current live pages: the page bookkeeping, the spill allocations and the cache entries. Some resources keep the size of the peak instead:

* The capacity of the dictionaries and queues, until the tree's client is disposed by a stop and start or a restart.
* Scratch buffers: the file cache's write buffer, the direct I/O writer's read buffer, serializer buffers, and zstd contexts with page compression. `Clear()` or an idle cleanup releases them.
* The physical length of the spill file. The file shrinks back only when its last allocation is freed, as at `Clear()`. With direct I/O on Linux the file stays on disk until the next writer with the same name is created.

Nothing shrinks back to the current size automatically.

The bounded spill holds for the built-in file cache. A custom `IFileCacheFactory` needs reads that complete synchronously and reuse of freed space for the same result, exactly as for ordinary trees.
