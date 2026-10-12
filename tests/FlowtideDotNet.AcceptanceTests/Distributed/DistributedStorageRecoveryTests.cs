using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.AcceptanceTests.Distributed;

public class DistributedStorageRecoveryTests
{
    private const string ConnectedSql = """
        SUBSTREAM a;
        CREATE VIEW v WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS SELECT userkey FROM users;
        SUBSTREAM b;
        INSERT INTO output SELECT userkey FROM v WITH (PARTITION_ID = 0);
        """;
    private sealed class NonListingProvider : MemoryFileProvider { public override bool SupportsFileListing => false; }

    private static FlowtideDotNet.Base.Engine.DataflowStream Build(string name, string member, IPersistentStorage? storage,
        LocalSubstreamCommunicationHub hub, string sql = ConnectedSql, Action<long>? commit = null)
    {
        var db = new MockDatabase();
        _ = new DatasetGenerator(db);
        var plan = new SqlPlanBuilder();
        plan.AddTableProvider(new DatasetTableProvider(db));
        plan.Sql(sql);
        var connectors = new ConnectorManager();
        connectors.AddSource(new MockSourceFactory("*", db, false));
        connectors.AddSink(new MockSinkFactory("*", _ => { }, 0, _ => { }, onCommitVersion: commit));
        return new FlowtideBuilder(name).AddPlan(plan.GetPlan(), false)
            .AddConnectorManager(connectors)
            .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
            .SetDistributedOptions(new DistributedOptions(member, null, hub.CreateFactory(member)))
            .Build();
    }

    [Fact]
    public void ConnectedGroupRejectsLegacyAndNonListingStorageDuringConstruction()
    {
        var legacy = Assert.Throws<NotSupportedException>(() => Build("legacy", "a", null, new()));
        Assert.Contains("Reservoir", legacy.Message);
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new NonListingProvider() });
        var nonListing = Assert.Throws<NotSupportedException>(() => Build("nonlisting", "a", storage, new()));
        Assert.Contains("file listing", nonListing.Message);
    }

    [Fact]
    public async Task DisconnectedSubstreamsDoNotRequireDistributedStorage()
    {
        await using var stream = Build("isolated", "a", null, new(),
            "SUBSTREAM a; INSERT INTO output SELECT userkey FROM users; SUBSTREAM b; INSERT INTO other SELECT userkey FROM users;");
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 20)]
    [InlineData(true, 20)]
    public async Task NeverCheckpointedMemberSelectsZeroAndBothMembersNextCommitOne(bool cached, int interval)
    {
        var parent = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "flowtide-zero-recovery"));
        var root = Path.GetFullPath(Path.Combine(parent, Guid.NewGuid().ToString("N")));
        Assert.StartsWith(parent + Path.DirectorySeparatorChar, root);
        ReservoirPersistentStorage Create(string member) => new(new ReservoirStorageOptions
        {
            FileProvider = new LocalDiskProvider(Path.Combine(root, member, "store")),
            CacheProvider = cached ? new LocalDiskProvider(Path.Combine(root, member, "cache")) : null,
            SnapshotCheckpointInterval = interval
        });
        var names = new[] { "zero-a", "zero-b" };
        try
        {
            using (var storage = Create("b"))
            using (var manager = new StateManagerSync<StreamState>(new StateManagerOptions { PersistentStorage = storage },
                NullLoggerFactory.Instance, new Meter("zero-seed"), names[1], GlobalMemoryManager.Instance))
            {
                await manager.InitializeAsync();
                await manager.CheckpointAsync();
            }
            // First recover 0/1 to zero. Then reconstruct both stores before any write;
            // the second start must still see zero, with or without a warmed cache.
            for (int reconstruction = 0; reconstruction < 2; reconstruction++)
            {
                using var aStorage = Create("a");
                using var bStorage = Create("b");
                var hub = new LocalSubstreamCommunicationHub();
                var commits = new ConcurrentQueue<long>();
                await using var a = Build(names[0], "a", aStorage, hub);
                await using var b = Build(names[1], "b", bStorage, hub, commit: commits.Enqueue);
                await Task.WhenAll(a.StartAsync(), b.StartAsync()).WaitAsync(TimeSpan.FromSeconds(20));
                await WaitUntil(() => a.State == StreamStateValue.Running && b.State == StreamStateValue.Running);
                Assert.Contains(0L, commits);
                Assert.Equal(1, aStorage.CurrentVersion);
                Assert.Equal(1, bStorage.CurrentVersion);
                if (reconstruction == 1)
                {
                    await a.TriggerCheckpoint();
                    await WaitUntil(() => commits.Contains(1L));
                    Assert.Equal(2, aStorage.CurrentVersion);
                    Assert.Equal(2, bStorage.CurrentVersion);
                }
            }
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!condition()) await Task.Delay(10, timeout.Token);
    }
}
