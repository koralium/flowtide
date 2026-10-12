using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Storage.Tests.Reservoir;

// Ordinary completed operations on built-in providers. No injected faults or engine hooks.
public class LocalDiskRecoveryTests
{
    public static IEnumerable<object[]> Cases()
    {
        foreach (var interval in new[] { 1, 5, 20 })
        foreach (var maxFileSize in new[] { 256, 64 * 1024 })
        foreach (var cached in new[] { false, true })
            yield return new object[] { interval, maxFileSize, cached };
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task PredecessorAndReplacementSurviveCompactionAndReconstruction(
        int interval, int maxFileSize, bool cached)
    {
        var parent = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "flowtide-plan-review-retention"));
        var root = Path.GetFullPath(Path.Combine(parent, Guid.NewGuid().ToString("N")));
        Assert.StartsWith(parent + Path.DirectorySeparatorChar, root);
        Directory.CreateDirectory(root);
        var metadata = new StorageInitializationMetadata(
            "retention-probe", NullLoggerFactory.Instance, GlobalMemoryManager.Instance);

        ReservoirPersistentStorage Create() => new(new ReservoirStorageOptions
        {
            FileProvider = new LocalDiskProvider(Path.Combine(root, "store")),
            CacheProvider = cached ? new LocalDiskProvider(Path.Combine(root, "cache")) : null,
            SnapshotCheckpointInterval = interval,
            MaxFileSize = maxFileSize
        });

        try
        {
            using (var storage = Create())
            {
                await storage.InitializeAsync(metadata);
                for (var cut = 1; cut <= 24; cut++)
                {
                    await WriteCut(storage, cut, 0);
                    await storage.CompactAsync(0, 0);
                }
                await AssertCut(storage, 24, 0); // Also warms the latest cut's data cache.
                await storage.RecoverAsync(23);
                await AssertCut(storage, 23, 0);
            }

            // No replacement checkpoint yet: truncation itself must survive reconstruction.
            using (var storage = Create())
            {
                await storage.InitializeAsync(metadata);
                await AssertCut(storage, 23, 0);
                await WriteCut(storage, 24, 1); // Reuses 24 with different bytes.
                await storage.CompactAsync(0, 0);
                await AssertCut(storage, 24, 1);
            }

            using (var storage = Create())
            {
                await storage.InitializeAsync(metadata);
                await AssertCut(storage, 24, 1);
            }
        }
        finally
        {
            // Only remove this test's checked, uniquely named temporary directory.
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    private static byte[] Payload(int cut, int branch)
    {
        var result = new byte[512];
        new Random(cut * 101 + branch).NextBytes(result);
        return result;
    }

    private static async Task WriteCut(ReservoirPersistentStorage storage, int cut, int branch)
    {
        using var session = storage.CreateSession();
        await session.Write(100, new SerializableObject(Payload(cut, branch)));
        if (cut == 1)
            await session.Write(200, new SerializableObject(Payload(1, 2)));
        if (cut % 2 == 1)
            await session.Write(300, new SerializableObject(Payload(cut, 3)));
        else
            await session.Delete(300);
        await session.Commit();
        await storage.CheckpointAsync(BitConverter.GetBytes(cut * 10 + branch), false);
    }

    private static async Task AssertCut(ReservoirPersistentStorage storage, int cut, int branch)
    {
        Assert.Equal((long)cut + 1, storage.CurrentVersion);
        Assert.True(storage.TryGetValue(1, out var metadata));
        Assert.Equal(BitConverter.GetBytes(cut * 10 + branch), metadata!.Value.ToArray());
        using var session = storage.CreateSession();
        Assert.Equal(Payload(cut, branch), (await session.Read(100)).ToArray());
        Assert.Equal(Payload(1, 2), (await session.Read(200)).ToArray());
        if (cut % 2 == 1)
            Assert.Equal(Payload(cut, 3), (await session.Read(300)).ToArray());
        else
            Assert.False(storage.TryGetValue(300, out _));
    }
}
