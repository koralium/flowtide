using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Storage.Tests;

// The default storage case shares the owned file cache path with other default storage tests.
[Collection("NonParallel")]
public class StateManagerRestoreTests
{
    private static StateManagerSync<string> CreateManager(IPersistentStorage storage) => new(new StateManagerOptions
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FileCacheRecoveryStartsEmpty(bool suppliedStorage)
    {
        var name = $"restore-filecache-{suppliedStorage}";
        using var storage = suppliedStorage ? new FileCachePersistentStorage(new FileCacheOptions { DirectoryPath = $"./data/{name}/persist" }) : null;
        using var manager = new StateManagerSync<string>(new StateManagerOptions
        {
            CachePageCount = 1000,
            MinCachePageCount = 100,
            PersistentStorage = storage,
            TemporaryStorageOptions = new FileCacheOptions { DirectoryPath = $"./data/{name}/temp" }
        }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
        await manager.InitializeAsync();
        for (long key = 1; key <= 3; key++)
        {
            var tree = await CreateTree(manager.GetOrCreateClient("node"));
            await tree.Upsert(key, key);
            await tree.Commit();
            await manager.CheckpointAsync();
            // Failure recovery passes the completed version.
            await manager.InitializeAsync(checkpointVersion: manager.LastCompletedCheckpointVersion);
            Assert.Equal(0, manager.LastCompletedCheckpointVersion);
            var (found, _) = await (await CreateTree(manager.GetOrCreateClient("node"))).GetValue(key);
            Assert.False(found);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReservoirRecoveryWithoutMetadataIsAnError(bool explicitVersion)
    {
        using var storage = new MetadataHidingStorage(new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() }));
        using var manager = CreateManager(storage);
        await manager.InitializeAsync();
        var tree = await CreateTree(manager.GetOrCreateClient("node"));
        await tree.Upsert(1, 1);
        await tree.Commit();
        await manager.CheckpointAsync();
        var completed = manager.LastCompletedCheckpointVersion;
        storage.HideMetadata = true;
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => explicitVersion ? manager.InitializeAsync(checkpointVersion: completed) : manager.InitializeAsync());
        Assert.Contains("missing its state manager metadata", e.Message);
    }

    private sealed class MetadataHidingStorage(IPersistentStorage inner) : IPersistentStorage
    {
        public bool HideMetadata { get; set; }
        public long CurrentVersion => inner.CurrentVersion;
        public Task InitializeAsync(StorageInitializationMetadata metadata) => inner.InitializeAsync(metadata);
        public IPersistentStorageSession CreateSession() => inner.CreateSession();
        public ValueTask CheckpointAsync(byte[] metadata, bool includeIndex) => inner.CheckpointAsync(metadata, includeIndex);
        public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount) => inner.CompactAsync(changesSinceLastCompact, pageCount);
        public ValueTask ResetAsync() => inner.ResetAsync();
        public ValueTask RecoverAsync(long checkpointVersion) => inner.RecoverAsync(checkpointVersion);
        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            if (HideMetadata && key == 1)
            {
                value = null;
                return false;
            }
            return inner.TryGetValue(key, out value);
        }
        public ValueTask Write(long key, byte[] value) => inner.Write(key, value);
        public void ClearForRestore() => inner.ClearForRestore();
        public void Dispose() => inner.Dispose();
    }
}
