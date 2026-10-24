// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A table the sweep inventories: its raw storage, the test view the meter wraps, and its configuration with both opt-ins
    internal sealed class OrphanTable
    {
        public required IFileStorage Raw { get; init; }

        public required TestStorage Owned { get; init; }

        public required CleanupConfiguration Configuration { get; init; }

        // As the sink names it
        public required string Name { get; init; }

        // The local disk variant's table folder
        public string? Folder { get; init; }

        // The table's full path with its terminal slash
        public string Prefix => Configuration.Store.Table.Full + "/";
    }

    // One frozen candidate as its payload reports it
    internal readonly record struct FrozenCandidate(string Relative, long? Size, DateTimeOffset Modified);

    [Collection(nameof(PinnedClientCollection))]
    public partial class OrphanSweepTests : IDisposable
    {
        internal static readonly DateTimeOffset Now = AdvanceExecutionKit.Now;
        internal static readonly TimeSpan Retention = AdvanceExecutionKit.Retention;
        internal static readonly TimeSpan Interval = TimeSpan.FromDays(7);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "os-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public OrphanSweepTests()
        {
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        // Own commits 0 to head publishing the retention, each adding one file and removing the one before
        internal static async Task RetainedTableAsync(IFileStorage storage, string table, long head)
        {
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(Retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(Retention)
            };
            for (long version = 0; version <= head; version++)
            {
                var add = DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet");
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published), add }
                    : new[] { add, DeltaLakeCheckpointBuildTests.Remove($"part-{version - 1:D4}.parquet", BoundarySelectorTests.Old(version)) };
                await DeltaTestKit.WriteOwnCommit(storage, table, version, 0, BoundarySelectorTests.Old(version), actions);
            }
        }

        // An empty eligible table at /t on local disk, below a new root unless given one
        private OrphanTable LocalTable(string? root = null)
        {
            root ??= Path.Combine(_directory, ScratchOwners.ShortId());
            var folder = Path.Combine(root, "t");
            Directory.CreateDirectory(folder);
            var raw = Files.Of.LocalDisk(root);
            _disposables.Add(raw);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(Retention, Interval));
            Assert.True(configuration.Store.Eligible);
            return new OrphanTable() { Raw = raw, Owned = new LocalTestStorage(raw), Configuration = configuration, Name = "t", Folder = folder };
        }

        // An empty eligible table below a non-root prefix on the loopback store, through the real pinned client
        private OrphanTable LoopbackTable(LoopbackObjectStore server, string name = "c/tables/t")
        {
            var client = server.CreateStorage();
            _disposables.Add(client);
            var configuration = CleanupConfiguration.Validate(client, name, false, new CleanupOptIns(Retention, Interval));
            Assert.True(configuration.Store.Eligible);
            return new OrphanTable() { Raw = client, Owned = new TestStorage(client), Configuration = configuration, Name = name };
        }

        // The table's view under an unthrottled maintenance meter
        internal static MeteredFileStorage Metered(OrphanTable table) => MeteredFileStorage.Wrap(table.Owned, MaintenanceBucketsTests.Unthrottled(), table.Configuration.Store.Store.Paging);

        // The sweep over the storage under an unthrottled meter
        internal static OrphanSweep Sweep(CleanupConfiguration configuration, IFileStorage storage, TimeProvider? clock = null, RetentionCounters? counters = null)
        {
            var metered = MeteredFileStorage.Wrap(storage, MaintenanceBucketsTests.Unthrottled(), configuration.Store.Store.Paging);
            return new OrphanSweep(configuration, metered, clock ?? new FakeTimeProvider(Now), NullLogger.Instance, configuration.Store.Table.Full.TrimStart('/'), counters ?? new RetentionCounters());
        }

        // A long orphan admission, with the worker's published version when given
        internal CleanupContext OrphanContext(long head, long? published = null, JobMemory? memory = null, long? diskCap = null, ProducerGenerations? producers = null,
            CapacityLedger? capacity = null)
        {
            var clock = new FakeTimeProvider(Now);
            return new CleanupContext(new JobRun(head, JobKind.Orphan) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Orphan,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(6),
                Head = head,
                PublishedNow = published is { } version ? () => version : null,
                TableId = null,
                Scratch = Scratch(diskCap ?? DiskLedger.DefaultCap),
                Memory = memory ?? new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default,
                Producers = producers ?? new ProducerGenerations(),
                Capacity = capacity
            };
        }

        // Takes every run the sweep registers, with one delete outstanding and no retry wait; the class disposes them
        internal List<OrphanRun> Registered(OrphanSweep sweep)
        {
            var runs = new List<OrphanRun>();
            sweep.Register = run =>
            {
                runs.Add(run);
                _disposables.Add(run);
            };
            sweep.RunSettings = static () => new DeletionRunSettings() { MaxOutstanding = 1, RetryDelay = TimeSpan.Zero };
            return runs;
        }

        // A run's targets by name below the table, in order
        internal static List<string> TargetsOf(OrphanRun run, OrphanTable table)
        {
            return run.Targets.ReadAllForTests().Select(x =>
            {
                Assert.StartsWith(table.Prefix, x.Path, StringComparison.Ordinal);
                return x.Path[table.Prefix.Length..];
            }).ToList();
        }

        // The key the sink's producers enter under
        internal static string ProducerKey(OrphanTable table) => TableLeases.KeyOf(table.Raw, table.Configuration.Store.Table);

        private JobScratch Scratch(long diskCap)
        {
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()), diskCap)).CreateJob();
            _disposables.Add(scratch);
            return scratch;
        }

        // Each frozen candidate in key order: its name below the table, its size and its modification time
        internal static List<FrozenCandidate> Frozen(OrphanInventory inventory, OrphanTable table)
        {
            var frozen = new List<FrozenCandidate>();
            using var reader = inventory.Candidates.Open();
            while (reader.MoveNext())
            {
                var path = Encoding.UTF8.GetString(reader.DeletionPath);
                Assert.StartsWith(table.Prefix, path, StringComparison.Ordinal);
                Assert.True(OrphanCandidatePayload.TryDecode(reader.Payload, out var size, out var modified, out _), path);
                frozen.Add(new FrozenCandidate(path[table.Prefix.Length..], size, modified));
            }
            return frozen;
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        // Local disk
        [InlineData(null)]
        public async Task PinnedStatAndListingCarryModificationTimes(StoreFlavor? flavor)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            var table = server != null ? LoopbackTable(server) : LocalTable();
            var metered = Metered(table);
            var reference = table.Prefix + ".flowtide-orphan-sweep";
            var commit = table.Prefix + "_delta_log/00000000000000000000.json";
            var old = new DateTimeOffset(2026, 9, 1, 6, 30, 15, TimeSpan.Zero);
            var written = new DateTimeOffset(2026, 10, 1, 8, 0, 1, TimeSpan.Zero);
            var committed = written.AddMinutes(5);

            // Each object write is stamped with the service's time, or local disk's own
            if (server != null)
            {
                server.Now = written;
            }
            await metered.WriteText(reference, "orphan sweep reference");
            if (server != null)
            {
                server.Now = committed;
            }
            await RetainedTableAsync(table.Raw, table.Name, 0);
            if (server != null)
            {
                server.Put("tables/t/part-old.parquet", new byte[3], old);
            }
            else
            {
                await File.WriteAllBytesAsync(Path.Combine(table.Folder!, "part-old.parquet"), new byte[3]);
                File.SetLastWriteTimeUtc(Path.Combine(table.Folder!, "part-old.parquet"), old.UtcDateTime);
                written = File.GetLastWriteTimeUtc(Path.Combine(table.Folder!, ".flowtide-orphan-sweep"));
                committed = File.GetLastWriteTimeUtc(Path.Combine(table.Folder!, "_delta_log", "00000000000000000000.json"));
            }
            var commitBytes = (await DeltaTestKit.ReadBytes(table.Raw, commit)).Length;

            var stat = await metered.Stat(reference);
            Assert.NotNull(stat);
            Assert.Equal(written, stat.LastModificationTime);
            Assert.Equal(Encoding.UTF8.GetByteCount("orphan sweep reference"), stat.Size);
            var commitStat = await metered.Stat(commit);
            Assert.NotNull(commitStat);
            Assert.Equal(committed, commitStat.LastModificationTime);
            Assert.Equal(commitBytes, commitStat.Size);

            var listed = (await metered.Ls(table.Prefix, recurse: true)).ToDictionary(x => x.Path.Full, StringComparer.Ordinal);
            Assert.Equal(written, listed[reference].LastModificationTime);
            Assert.Equal(Encoding.UTF8.GetByteCount("orphan sweep reference"), listed[reference].Size);
            Assert.Equal(committed, listed[commit].LastModificationTime);
            Assert.Equal(commitBytes, listed[commit].Size);
            Assert.Equal(old, listed[table.Prefix + "part-old.parquet"].LastModificationTime);
            Assert.Equal(3, listed[table.Prefix + "part-old.parquet"].Size);
        }
    }
}
