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

```
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
This is usually done in the `OnCheckpoint` method in an operator.
But if the tree is used to store temporary data, `Commit` should not be called.

Example:

```csharp
public override async Task<OperatorState> OnCheckpoint()
{
    await _tree.Commit();
    return new OperatorState();
}
```