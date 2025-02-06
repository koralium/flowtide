---
sidebar_position: 2
---

# Object State

The object state is the simplest type of state for an operator, it saves a C# object using JSON serialization to store it in persistent storage.

An object state can be fetched from the `IStateManagerClient` using `GetOrCreateObjectStateAsync<T>(string name)`.

Example:

```csharp
protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
{
    _state = await stateManagerClient.GetOrCreateObjectStateAsync<MyState>("my_state");
}

protected override async Task OnCheckpoint()
{
    // Commit any changes made to the state
    await _state.Commit();
}

public void OtherMethod()
{
    _state.Value.Test = "hello";
}
```

The object state saves an internal copy of the value that was last commited and checkpointed, if the value has not changed nothing will be written to persistent storage.
So a user of the object state does not have to handle conditional calls to `Commit` to reduce the number of writes to persistent storage.