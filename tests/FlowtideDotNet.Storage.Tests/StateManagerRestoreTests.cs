using System.Diagnostics.Metrics;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Storage.Tests;

public class StateManagerRestoreTests
{
    private static StateManagerSync<string> CreateManager(ReservoirPersistentStorage storage) => new(new StateManagerOptions
    {
        CachePageCount = 1000,
        MinCachePageCount = 100,
        PersistentStorage = storage
    }, NullLoggerFactory.Instance, new Meter("restore-tests"), "restore-tests", GlobalMemoryManager.Instance);

    private static ValueTask<IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>> CreateTree(IStateManagerClient client) =>
        client.GetOrCreateTree("tree", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>
        {
            Comparer = new PrimitiveListComparer<long>(),
            KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
            ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
            MemoryAllocator = GlobalMemoryManager.Instance
        });

    [Theory]
    [InlineData(1)]
    [InlineData(20)]
    public async Task DownwardRestoreReloadsMetadataPagesAndClientBindings(int snapshotInterval)
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions
        {
            FileProvider = new MemoryFileProvider(), SnapshotCheckpointInterval = snapshotInterval
        });
        using var manager = CreateManager(storage);
        await manager.InitializeAsync();
        var client = manager.GetOrCreateClient("node");
        var state = await client.GetOrCreateObjectStateAsync<string>("state");
        var tree = await CreateTree(client);
        state.Value = "one";
        manager.Metadata = "time-one";
        await tree.Upsert(1, 10);
        await state.Commit();
        await tree.Commit();
        await manager.CheckpointAsync();

        var later = await client.GetOrCreateObjectStateAsync<string>("later");
        later.Value = "discard";
        state.Value = "two";
        manager.Metadata = "time-two";
        await tree.Upsert(1, 20);
        await later.Commit();
        await state.Commit();
        await tree.Commit();
        await manager.CheckpointAsync();

        await manager.InitializeAsync(checkpointVersion: 1);
        Assert.Equal(1, manager.LastCompletedCheckpointVersion);
        Assert.Equal(2, manager.CurrentVersion);
        Assert.Equal("time-one", manager.Metadata);
        Assert.Equal("one", state.Value);
        Assert.Null(later.Value);
        tree = await CreateTree(client);
        Assert.Equal((true, 10L), await tree.GetValue(1));

        // Reuse the rolled-back client and allocate pages after it. A fresh manager
        // must discover both clients using the replacement checkpoint's bindings.
        later.Value = "replacement";
        await later.Commit();
        await tree.Upsert(2, 30);
        await tree.Commit();
        await manager.CheckpointAsync();
        manager.Dispose();
        using var reconstructed = CreateManager(storage);
        await reconstructed.InitializeAsync();
        client = reconstructed.GetOrCreateClient("node");
        Assert.Equal("replacement", (await client.GetOrCreateObjectStateAsync<string>("later")).Value);
        Assert.Equal("one", (await client.GetOrCreateObjectStateAsync<string>("state")).Value);
        Assert.Equal((true, 30L), await (await CreateTree(client)).GetValue(2));
        Assert.Equal(2, reconstructed.LastCompletedCheckpointVersion);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(20, 1)]
    [InlineData(1, 4)]
    [InlineData(20, 4)]
    public async Task ZeroRestoreAndReconstructionPreserveFreshNumberingAndClientBindings(int snapshotInterval, int checkpoints)
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions
        {
            FileProvider = new MemoryFileProvider(), SnapshotCheckpointInterval = snapshotInterval
        });
        using var manager = CreateManager(storage);
        await manager.InitializeAsync();
        var state = await manager.GetOrCreateClient("node").GetOrCreateObjectStateAsync<string>("state");
        state.Value = "old";
        await state.Commit();
        for (int i = 0; i < checkpoints; i++) await manager.CheckpointAsync();
        await manager.InitializeAsync(checkpointVersion: 0);
        Assert.Equal(0, manager.LastCompletedCheckpointVersion);
        Assert.Equal(1, manager.CurrentVersion);
        Assert.Null(manager.Metadata);
        Assert.Null(state.Value);
        // Reopen before writing anything to ensure reset itself retired the timeline.
        await manager.InitializeAsync();
        Assert.Equal(0, manager.LastCompletedCheckpointVersion);
        Assert.Equal(1, manager.CurrentVersion);
        state.Value = "new";
        await state.Commit();
        await manager.CheckpointAsync();
        manager.Dispose();
        using var reconstructed = CreateManager(storage);
        await reconstructed.InitializeAsync();
        Assert.Equal(1, reconstructed.LastCompletedCheckpointVersion);
        Assert.Equal("new", (await reconstructed.GetOrCreateClient("node").GetOrCreateObjectStateAsync<string>("state")).Value);
    }

    [Fact]
    public async Task RequestedMissingCheckpointIsAnErrorWithoutResettingTheStore()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        using var manager = CreateManager(storage);
        await Assert.ThrowsAnyAsync<Exception>(() => manager.InitializeAsync(checkpointVersion: 1));
        Assert.False(manager.Initialized);
    }
}
