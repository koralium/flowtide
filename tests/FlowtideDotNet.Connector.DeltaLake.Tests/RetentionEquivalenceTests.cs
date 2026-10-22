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

using DuckDB.NET.Data;
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Core.ColumnStore;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RetentionEquivalenceTests : IDisposable
    {
        private const long Versions = 40;

        private readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();

        public void Dispose()
        {
            _kit.Dispose();
        }

        // The metadata with the kit's retention published
        private static DeltaAction Published(DeltaMetadataAction current)
        {
            var retention = RetentionProperties.Format(AdvanceExecutionKit.Retention);
            return new DeltaAction()
            {
                MetaData = new DeltaMetadataAction()
                {
                    Id = current.Id,
                    Name = current.Name,
                    Description = current.Description,
                    Format = current.Format,
                    SchemaString = current.SchemaString,
                    PartitionColumns = current.PartitionColumns,
                    CreatedTime = current.CreatedTime,
                    Configuration = new Dictionary<string, string>(current.Configuration ?? new Dictionary<string, string>())
                    {
                        [RetentionProperties.LogKey] = retention,
                        [RetentionProperties.DeletedFileKey] = retention
                    }
                }
            };
        }

        // A copy of the table's commits alone, which our reader replays from the first commit
        private string FullHistory(string native)
        {
            var root = _kit.NewRoot();
            var log = Directory.CreateDirectory(Path.Combine(root, "t", "_delta_log")).FullName;
            foreach (var file in Directory.EnumerateFiles(Path.Combine(native, "_delta_log")).Where(x => Regex.IsMatch(Path.GetFileName(x), @"^\d{20}\.json$")))
            {
                File.Copy(file, Path.Combine(log, Path.GetFileName(file)));
            }
            return root;
        }

        [Theory]
        [InlineData(3)]
        [InlineData(19)]
        [InlineData(47)]
        public async Task RetainedVersionsMatchFullHistory(int seed)
        {
            var root = _kit.NewRoot();
            var table = _kit.Open(root, Versions);
            var native = Path.Combine(root, "t");
            var planned = new AdvancePlannerKit.Table() { Local = table.Raw, Configuration = table.Configuration, Native = native };
            await AdvancePlannerKit.WriteHistory(planned, seed, Versions, schemaAndTxn: true);
            await AdvancePlannerKit.Commit(planned, Versions, 0, Published((await DeltaTransactionReader.ReadTable(table.Raw, "t"))!.Metadata));
            // The boundary, the newest checkpoint, at a commit that removes a file it does not add again
            long c = -1;
            for (var version = 30L; version >= 24 && c < 0; version--)
            {
                var actions = await ReadCommitActions(table.Raw, "t", version);
                c = actions.Any(x => x.Remove != null && !actions.Any(y => y.Add?.Path == x.Remove.Path)) ? version : -1;
            }
            Assert.True(c > 0, "No commit from 24 to 30 removes a file");
            await _kit.BuildAsync(table.Raw, 13);
            await _kit.BuildAsync(table.Raw, c);
            // A hint left at the older checkpoint
            await new StowageCheckpointTransport(table.Raw, "t").WriteHintAsync(new LastCheckpointInfo() { Version = 13, Size = 1 }, default);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(native);
            using var full = Files.Of.LocalDisk(FullHistory(native));
            var history = new Dictionary<long, VersionSnapshot>();
            for (long version = 1; version <= table.Head; version++)
            {
                history[version] = VersionSnapshot.Of((await DeltaTransactionReader.ReadTable(full, "t", version))!);
            }
            // Schema and transactions change below C and again above it
            Assert.NotEqual(history[1].Schema, history[c].Schema);
            Assert.NotEqual(history[c].Schema, history[table.Head].Schema);
            Assert.NotEqual("", history[c].Transactions);
            Assert.NotEqual(history[c].Transactions, history[table.Head].Transactions);
            var baseline = await Invariants.CaptureAsync(table, c, table.Head);
            Assert.NotEmpty(baseline.Oracle!.Retired);

            // All of history is past the cutoff
            var cleanup = AdvanceExecutionKit.Cleanup(table, null, new FakeTimeProvider(AdvancePlannerKit.Now + AdvanceExecutionKit.Retention));
            var outcome = new CellOutcome(table);
            outcome.Attach(cleanup);
            cleanup.Settings = cleanup.Settings with { MaxPaths = DeletionRunSettings.DefaultMaxPaths };
            var ledger = new CapacityLedger(() => default);
            outcome.Ledger = ledger;
            var started = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.True(started.Kind == OutcomeKind.Completed, $"{started.Kind} {started.Reason}: {started.Detail}");
            Assert.Equal(c, cleanup.ActiveForTests!.C);
            await _kit.ExecuteAsync(cleanup, table, ledger);

            await Invariants.AssertAsync(table, baseline, outcome);
            // Every retained version as the full history reads it, tombstones excluded
            for (var version = c; version <= table.Head; version++)
            {
                Assert.Equal(history[version], VersionSnapshot.Of((await DeltaTransactionReader.ReadTable(table.Raw, "t", version))!));
            }
            // C's commit and checkpoint stay, nothing below C does, and the hint and the oldest version are C or above
            var listing = await DeltaTransactionReader.ListLog(table.Raw, "t");
            Assert.Equal(Enumerable.Range((int)c, (int)(table.Head - c + 1)).Select(x => (long)x), listing.Commits.Keys.Order());
            Assert.Equal(new[] { c }, listing.Checkpoints.Select(x => x.Version));
            var hint = await DeltaMaintenanceWorkerTests.Hint(table.Raw);
            Assert.True(hint != null && hint.Version >= c && listing.Checkpoints.Any(x => x.Version == hint.Version), $"The hint names {hint?.Version}");
            Assert.Equal(c, (await DeltaTransactionReader.ReadTable(table.Raw, "t", 0))!.Version);
        }
    }

    // Sink-written tables, change data off and on, kept or reshaped, built once per class
    public sealed class SinkHistories : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "rr-" + ScratchOwners.ShortId());
        private readonly Dictionary<(bool Cdc, bool Reshaped), Task<SinkHistory>> _histories = new Dictionary<(bool Cdc, bool Reshaped), Task<SinkHistory>>();

        internal Task<SinkHistory> GetAsync(bool cdc, bool reshaped)
        {
            lock (_histories)
            {
                if (!_histories.TryGetValue((cdc, reshaped), out var history))
                {
                    history = SinkHistory.WriteAsync(Path.Combine(_directory, (cdc ? "cdc" : "plain") + (reshaped ? "-reshaped" : "")), cdc, reshaped);
                    _histories.Add((cdc, reshaped), history);
                }
                return history;
            }
        }

        public void Dispose()
        {
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
    }

    // A sink-written table after its advances, a mirror of every commit and file it ever had, and source states saved at C - 1
    internal sealed class SinkHistory
    {
        public static readonly TimeSpan Guard = TimeSpan.FromMinutes(1);
        public static readonly Action<DeltaLakeOptions> Changes = options => options.IncludeChangeHistory = true;

        private const string Write = @"
            CREATE TABLE t (
                userkey INT,
                name STRING
            );

            INSERT INTO t
            SELECT userKey AS userkey, firstName AS name FROM users
        ";

        // A new stream's overwrite with the column another writer added
        private const string Overwrite = @"
            CREATE TABLE t (
                userkey INT,
                name STRING,
                visits INT
            );

            INSERT OVERWRITE t
            SELECT userKey AS userkey, firstName AS name, visits FROM users
        ";

        // The other writer's transaction id
        private const string OtherWriter = "other-writer";

        // The table at its root
        public required string Live { get; init; }

        // The mirror at its root, without checkpoints
        public required string Full { get; init; }

        public required long C { get; init; }

        public required long Head { get; init; }

        // The columns the sources read, in every file they replay
        public required IReadOnlyList<string> Columns { get; init; }

        // A plain source's state at C - 1
        public required KeepAliveMemoryFileProvider RowState { get; init; }

        // Two change history sources' states at C - 1, one resumed over the table and one over the mirror
        public required KeepAliveMemoryFileProvider LiveChangeState { get; init; }

        public required KeepAliveMemoryFileProvider FullChangeState { get; init; }

        public string Read => Query(Columns, false);

        public string ReadChanges => Query(Columns, true);

        private static string Query(IReadOnlyList<string> columns, bool changes) => $"INSERT INTO result SELECT {string.Join(", ", columns)}{(changes ? ", __weight, __modificationDate, __version" : "")} FROM t";

        // Commits from midnight on under a day of retention; kept, advanced to C = 1 and then to C = 3; reshaped, advanced to an overwrite at C = 4 between two added columns
        public static async Task<SinkHistory> WriteAsync(string directory, bool cdc, bool reshaped)
        {
            var live = Path.Combine(directory, "live");
            var full = Path.Combine(directory, "full");
            Directory.CreateDirectory(live);
            Directory.CreateDirectory(full);
            var name = $"{nameof(SinkHistory)}_{(cdc ? "cdc" : "plain")}{(reshaped ? "_reshaped" : "")}";
            var columns = reshaped ? new[] { "userkey", "name", "visits" } : new[] { "userkey", "name" };
            var states = new[] { new KeepAliveMemoryFileProvider(), new KeepAliveMemoryFileProvider(), new KeepAliveMemoryFileProvider() };
            using var storage = Files.Of.LocalDisk(live);
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
            long version = 0;
            DeltaLakeSinkStream? writer = null;

            async Task CommitAsync()
            {
                Change(writer!, ++version);
                await WaitForVersion(storage, "t", writer!, version);
                Mirror(live, full);
            }

            // Every source reads the table at the version
            async Task SaveAllAsync()
            {
                await SaveAsync($"{name}_rows", live, states[0], columns, false, version);
                await SaveAsync($"{name}_changes", live, states[1], columns, true, version);
                await SaveAsync($"{name}_fullchanges", live, states[2], columns, true, version);
            }

            await using (var stream = Sink(name, storage, clock, cdc))
            {
                writer = stream;
                // One file that takes a deletion vector at each change
                stream.Generate(200);
                await stream.StartStream(Write);
                await WaitForVersion(storage, "t", stream, version);
                Mirror(live, full);
                await stream.Maintenance.TickAndSettleAsync(version);

                // Day one: two commits
                clock.Advance(TimeSpan.FromDays(1));
                await CommitAsync();
                await CommitAsync();
                await stream.Maintenance.TickAndSettleAsync(version);
                if (!reshaped)
                {
                    // Read by every source before the third
                    await SaveAllAsync();

                    // Day two: a cutoff build at 1
                    clock.Advance(TimeSpan.FromDays(1));
                    await CommitAsync();
                    await stream.Maintenance.TickAndSettleAsync(version);

                    // Day three: the advance to 1, then a cutoff build at 3
                    clock.Advance(TimeSpan.FromDays(1));
                    await CommitAsync();
                    await stream.Maintenance.TickAndSettleAsync(version);
                    Assert.Equal(1, await DoneAsync(storage));
                    Assert.Equal(new long[] { 1, 3 }, await DeltaMaintenanceWorkerTests.Checkpoints(storage));

                    // Day four: three commits, the advance to 3, then a cutoff build at 4
                    clock.Advance(TimeSpan.FromDays(1));
                    for (int i = 0; i < 3; i++)
                    {
                        await CommitAsync();
                    }
                    await AdvancedAsync(stream, storage, version, 3);
                }
            }
            if (reshaped)
            {
                // A minute before day two another writer adds a column and every source reads it; at midnight a new stream overwrites the table in an epoch of its own
                clock.Advance(TimeSpan.FromDays(1) - TimeSpan.FromMinutes(1));
                await WidenAsync(storage, ++version, clock, "visits");
                Mirror(live, full);
                await SaveAllAsync();
                clock.Advance(TimeSpan.FromMinutes(1));
                await using (var stream = Sink($"{name}_overwrite", storage, clock, cdc))
                {
                    writer = stream;
                    stream.Generate(200);
                    await stream.StartStream(Overwrite);
                    await WaitForVersion(storage, "t", stream, ++version);
                    Mirror(live, full);
                    await stream.Maintenance.TickAndSettleAsync(version);

                    // Day three: a change, then a cutoff build at the overwrite
                    clock.Advance(TimeSpan.FromDays(1));
                    await CommitAsync();
                    await stream.Maintenance.TickAndSettleAsync(version);

                    // Day four: a change, the advance to the overwrite, then a cutoff build at 5
                    clock.Advance(TimeSpan.FromDays(1));
                    await CommitAsync();
                    await AdvancedAsync(stream, storage, version, 4);
                }
                // Another column above C, the head
                await WidenAsync(storage, ++version, clock, "note");
                Mirror(live, full);
            }
            var c = reshaped ? 4 : 3;
            Assert.Equal(7, (await DeltaTransactionReader.ListLog(storage, "t")).Head);
            // The advances deleted payload the mirror keeps
            Assert.NotEmpty(DataFiles(full).Except(DataFiles(live)));
            await AssertShapeAsync(storage, full, cdc, reshaped, c);
            return new SinkHistory() { Live = live, Full = full, C = c, Head = 7, Columns = columns, RowState = states[0], LiveChangeState = states[1], FullChangeState = states[2] };
        }

        private static DeltaLakeSinkStream Sink(string name, IFileStorage storage, TimeProvider clock, bool cdc)
        {
            var stream = new DeltaLakeSinkStream(name, storage, options =>
            {
                options.HistoryRetention = TimeSpan.FromDays(1);
                options.CheckpointInterval = 0;
                options.TimeProvider = clock;
                options.WriteChangeDataOnNewTables = cdc;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            return stream;
        }

        // The advance to C and a cutoff build after it, without a failed job
        private static async Task AdvancedAsync(DeltaLakeSinkStream stream, IFileStorage storage, long version, long c)
        {
            await stream.Maintenance.TickAndSettleAsync(version);
            Assert.Equal(c, await DoneAsync(storage));
            Assert.Equal(new long[] { c, c + 1 }, await DeltaMaintenanceWorkerTests.Checkpoints(storage));
            var worker = stream.Maintenance.Workers.Single();
            Assert.Equal((0, 0), (worker.CleanupFailures(JobKind.Advance), worker.CleanupFailures(JobKind.Deletion)));
        }

        // Another writer's commit that adds a nullable integer column under column mapping and counts its own transaction
        private static async Task WidenAsync(IFileStorage storage, long version, TimeProvider clock, string column)
        {
            var current = (await DeltaTransactionReader.ReadTable(storage, "t", version - 1))!;
            var schema = JsonNode.Parse(current.Metadata.SchemaString!)!.AsObject();
            var fields = schema["fields"]!.AsArray();
            var id = fields.Max(x => x!["metadata"]!["delta.columnMapping.id"]!.GetValue<int>()) + 1;
            fields.Add(new JsonObject()
            {
                ["name"] = column,
                ["type"] = "integer",
                ["nullable"] = true,
                ["metadata"] = new JsonObject() { ["delta.columnMapping.id"] = id, ["delta.columnMapping.physicalName"] = $"col-{Guid.NewGuid()}" }
            });
            var widened = DeltaMetadataCopy.Copy(current.Metadata, null);
            widened.SchemaString = schema.ToJsonString();
            widened.Configuration!["delta.columnMapping.maxColumnId"] = id.ToString(CultureInfo.InvariantCulture);
            var transaction = current.Transactions.TryGetValue(OtherWriter, out var previous) ? previous.Version + 1 : 1;
            var now = clock.GetUtcNow().ToUnixTimeMilliseconds();
            await WriteCommit(storage, "t", version,
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Timestamp = now, Operation = "ADD COLUMNS" } },
                new DeltaAction() { MetaData = widened },
                new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = OtherWriter, Version = transaction, LastUpdated = now } });
        }

        // Deletion vectors and change data; reshaped, C's overwrite and an added column on each side of C, else removed files and a file live at C that no commit from C on names
        private static async Task AssertShapeAsync(IFileStorage live, string full, bool cdc, bool reshaped, long c)
        {
            const long head = 7;
            var atC = (await DeltaTransactionReader.ReadTable(live, "t", c))!;
            var atHead = (await DeltaTransactionReader.ReadTable(live, "t", head))!;
            Assert.Contains(atHead.AddFiles, x => x.DeletionVector != null);
            if (reshaped)
            {
                Assert.Equal(cdc, (await ReadCommitActions(live, "t", c + 1)).Any(x => x.Cdc != null));
                // C removes every file of the version before it, kept for change replay from C - 1
                using var mirror = Files.Of.LocalDisk(full);
                var overwrite = await ReadCommitActions(live, "t", c);
                foreach (var add in (await DeltaTransactionReader.ReadTable(mirror, "t", c - 1))!.AddFiles)
                {
                    Assert.Contains(overwrite, x => x.Remove?.Path == add.Path);
                    Assert.True(await live.Exists($"/t/{add.Path}"), add.Path);
                }
                Assert.Equal(new[] { "userkey", "name", "visits" }, atC.Schema.Fields.Select(x => x.Name));
                Assert.Equal(new[] { "userkey", "name", "visits", "note" }, atHead.Schema.Fields.Select(x => x.Name));
                Assert.Equal((1L, 2L), (atC.Transactions[OtherWriter].Version, atHead.Transactions[OtherWriter].Version));
                return;
            }
            Assert.Equal(cdc, (await ReadCommitActions(live, "t", c)).Any(x => x.Cdc != null));
            Assert.Contains(await ReadCommitActions(live, "t", 6), x => x.Remove != null && x.Remove.DeletionVector == null);
            var named = new HashSet<string>();
            for (var version = c; version <= head; version++)
            {
                named.UnionWith((await ReadCommitActions(live, "t", version)).Select(x => x.Add?.Path ?? x.Remove?.Path ?? x.Cdc?.Path).OfType<string>());
            }
            Assert.Contains(atC.AddFiles, x => !named.Contains(x.Path!));
        }

        // Deletes a first-commit user, renames another and adds three; version 6 also deletes the newest user
        private static void Change(DeltaLakeSinkStream stream, long version)
        {
            var users = stream.Users.ToList();
            var renamed = users[50 + (int)version];
            stream.DeleteUser(users[(int)version]);
            if (version == 6)
            {
                stream.DeleteUser(users[^1]);
            }
            stream.AddOrUpdateUser(new User()
            {
                UserKey = renamed.UserKey,
                Gender = renamed.Gender,
                FirstName = $"renamed-{version}",
                LastName = renamed.LastName,
                NullableString = renamed.NullableString,
                CompanyId = renamed.CompanyId,
                Visits = renamed.Visits,
                ManagerKey = renamed.ManagerKey,
                TrimmableNullableString = renamed.TrimmableNullableString,
                DoubleValue = renamed.DoubleValue,
                Active = renamed.Active,
                BirthDate = renamed.BirthDate
            });
            stream.Generate(3);
        }

        // Every commit and every file outside the log the mirror lacks
        private static void Mirror(string live, string full)
        {
            var table = Path.Combine(live, "t");
            var log = Path.Combine(table, "_delta_log") + Path.DirectorySeparatorChar;
            foreach (var file in Directory.EnumerateFiles(table, "*", SearchOption.AllDirectories))
            {
                if (file.StartsWith(log, StringComparison.Ordinal) && !Regex.IsMatch(Path.GetFileName(file), @"^\d{20}\.json$"))
                {
                    continue;
                }
                var target = Path.Combine(full, Path.GetRelativePath(live, file));
                if (!File.Exists(target))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    File.Copy(file, target);
                }
            }
        }

        private static IEnumerable<string> DataFiles(string root)
        {
            var table = Path.Combine(root, "t");
            return Directory.EnumerateFiles(table, "*", SearchOption.AllDirectories).Select(x => Path.GetRelativePath(table, x)).Where(x => !x.StartsWith("_delta_log", StringComparison.Ordinal));
        }

        // The boundary of the only cleanup record left, a done
        private static async Task<long> DoneAsync(IFileStorage storage)
        {
            var done = Assert.Single((await DeltaTransactionReader.ListLog(storage, "t")).Controls);
            Assert.True(ControlName.TryParse(done.Name, out var name) && name.Kind == ControlKind.Done, done.Name);
            return name.Boundary;
        }

        // A source that read the table at the version and checkpointed, then stopped
        private static async Task SaveAsync(string name, string root, KeepAliveMemoryFileProvider state, IReadOnlyList<string> columns, bool changes, long version)
        {
            using var storage = Files.Of.LocalDisk(root);
            var expected = (await RetainedRows.RowsAtAsync(storage, version, columns)).Rows;
            await using var source = new DeltaLakeTestStream(name, storage, oneVersionPerCheckpoint: false, stateFiles: state, configure: changes ? Changes : null);
            await source.StartStream(Query(columns, changes));
            await UntilAsync(source, rows => rows.Select(x => string.Join("|", x.Split('|').Take(columns.Count))).Order(StringComparer.Ordinal).SequenceEqual(expected)
                && rows.All(x => !changes || VersionOf(x) == version));
            await source.StopStream();
        }

        // Published results until the rows hold
        public static async Task<List<string>> UntilAsync(DeltaLakeTestStream stream, Func<List<string>, bool> holds)
        {
            var rows = new List<string>();
            for (int update = 0; update < 100; update++)
            {
                await stream.WaitForUpdate().WaitAsync(Guard);
                try
                {
                    rows = RowsOf(stream.GetActualRowsAsVectors());
                }
                catch (Exception)
                {
                    // Replaced by the next result
                    continue;
                }
                if (holds(rows))
                {
                    return rows;
                }
            }
            Assert.Fail($"The rows never held: {string.Join(", ", rows)}");
            return rows;
        }

        // Each row's values in the reader kit's form, in ordinal order
        public static List<string> RowsOf(EventBatchData data)
        {
            var rows = new List<string>();
            for (int i = 0; i < data.Count; i++)
            {
                rows.Add(string.Join("|", Enumerable.Range(0, data.Columns.Count).Select(x => data.Columns[x].GetValueAt(i, default).ToString())));
            }
            rows.Sort(StringComparer.Ordinal);
            return rows;
        }

        // The version of a change history row
        public static long VersionOf(string row) => long.Parse(row.Split('|')[^1], CultureInfo.InvariantCulture);
    }

    public class RetainedRowEquivalenceTests : IClassFixture<SinkHistories>
    {
        private readonly SinkHistories _histories;
        private readonly ITestOutputHelper _output;

        public RetainedRowEquivalenceTests(SinkHistories histories, ITestOutputHelper output)
        {
            _histories = histories;
            _output = output;
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task RetainedRowsMatchFullHistory(bool cdc, bool reshaped)
        {
            var history = await _histories.GetAsync(cdc, reshaped);
            using var live = Files.Of.LocalDisk(history.Live);
            using var full = Files.Of.LocalDisk(history.Full);

            // The oldest version is C, whose predecessor's commit is gone
            Assert.Equal(history.C, (await DeltaTransactionReader.ReadTable(live, "t", 0))!.Version);
            Assert.False(await live.Exists(CommitPath("t", history.C - 1)));
            var distinct = new HashSet<string>();
            for (var version = history.C; version <= history.Head; version++)
            {
                var expected = await RetainedRows.RowsAtAsync(full, version);
                var read = await RetainedRows.RowsAtAsync(live, version);
                Assert.Equal((version, expected.Schema), (read.Version, read.Schema));
                Assert.Equal(expected.Rows, read.Rows);
                // Its files, protocol, metadata and transactions too
                Assert.Equal(VersionSnapshot.Of((await DeltaTransactionReader.ReadTable(full, "t", version))!), VersionSnapshot.Of((await DeltaTransactionReader.ReadTable(live, "t", version))!));
                distinct.Add(string.Join("\n", read.Rows));
            }
            // Each retained version has rows of its own
            Assert.Equal(history.Head - history.C + 1, distinct.Count);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task FreshBootstrapFromBoundary(bool cdc, bool reshaped)
        {
            var history = await _histories.GetAsync(cdc, reshaped);
            using var live = Files.Of.LocalDisk(history.Live);
            using var full = Files.Of.LocalDisk(history.Full);
            var storage = new HookFileStorage(live);
            await using var source = new DeltaLakeTestStream($"{nameof(FreshBootstrapFromBoundary)}_{cdc}_{reshaped}", storage, oneVersionPerCheckpoint: true, recordPublished: true);
            var logs = new TestLogCollector();
            source.AddLoggerProvider(logs);
            await source.StartStream(history.Read);

            // The first snapshot is C's, then one per version whose rows differ from the version before
            var published = 0;
            IReadOnlyList<string>? previous = null;
            for (var version = history.C; version <= history.Head; version++)
            {
                var expected = (await RetainedRows.RowsAtAsync(full, version, history.Columns)).Rows;
                if (previous != null && expected.SequenceEqual(previous))
                {
                    continue;
                }
                previous = expected;
                var rows = await source.WaitForPublished(++published).WaitAsync(SinkHistory.Guard);
                Assert.Equal(expected, SinkHistory.RowsOf(rows));
            }
            // Every kept version has rows of its own; reshaped, the head only adds a column
            Assert.Equal(reshaped ? 3 : 5, published);
            await PastHeadAsync(source, storage, logs, history.Head);
            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
        }

        // Until the source polled twice past the head or logged an error
        private static async Task PastHeadAsync(DeltaLakeTestStream source, HookFileStorage storage, TestLogCollector logs, long head)
        {
            var next = $"Exists {CommitPath("t", head + 1)}";
            var deadline = DateTime.UtcNow + SinkHistory.Guard;
            while (storage.Requests.Count(x => x == next) < 2 && logs.Errors.Count == 0)
            {
                Assert.True(DateTime.UtcNow < deadline, $"The source never read past version {head}");
                await source.SchedulerTick();
                await Task.Delay(10);
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task ResumeFromBeforeBoundary(bool cdc, bool reshaped)
        {
            var history = await _histories.GetAsync(cdc, reshaped);
            using var full = Files.Of.LocalDisk(history.Full);

            // A plain source continues at C's commit and ends at the head's rows
            using (var live = Files.Of.LocalDisk(history.Live))
            {
                var storage = new HookFileStorage(live);
                var expected = (await RetainedRows.RowsAtAsync(full, history.Head, history.Columns)).Rows;
                await using var source = new DeltaLakeTestStream($"{nameof(ResumeFromBeforeBoundary)}_{cdc}_{reshaped}_rows", storage, oneVersionPerCheckpoint: false, stateFiles: history.RowState);
                var logs = new TestLogCollector();
                source.AddLoggerProvider(logs);
                await source.StartStream(history.Read);
                await SinkHistory.UntilAsync(source, rows => rows.SequenceEqual(expected));
                Assert.Contains($"OpenRead {CommitPath("t", history.C)}", storage.Requests);
                await PastHeadAsync(source, storage, logs, history.Head);
                Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
            }

            // Change history sources over the table and over the mirror send the same rows, C's changes among them
            var changes = await ChangesAsync($"{nameof(ResumeFromBeforeBoundary)}_{cdc}_{reshaped}_live", history.Live, history.LiveChangeState, history);
            var mirrored = await ChangesAsync($"{nameof(ResumeFromBeforeBoundary)}_{cdc}_{reshaped}_full", history.Full, history.FullChangeState, history);
            Assert.Equal(mirrored, changes);
            Assert.Contains(changes, x => SinkHistory.VersionOf(x) == history.C);
            Assert.All(changes, x => Assert.InRange(SinkHistory.VersionOf(x), history.C - 1, history.Head));
        }

        // A change history source's rows once it sent the newest changes and read past the head, with no halt
        private static async Task<List<string>> ChangesAsync(string name, string root, KeepAliveMemoryFileProvider state, SinkHistory history)
        {
            using var local = Files.Of.LocalDisk(root);
            var newest = history.Head;
            while (!DeltaLakeSource.HasDataChange((await DeltaTransactionReader.ReadVersionCommit(local, "t", newest))!))
            {
                newest--;
            }
            var storage = new HookFileStorage(local);
            await using var source = new DeltaLakeTestStream(name, storage, oneVersionPerCheckpoint: false, stateFiles: state, configure: SinkHistory.Changes);
            var logs = new TestLogCollector();
            source.AddLoggerProvider(logs);
            await source.StartStream(history.ReadChanges);
            var rows = await SinkHistory.UntilAsync(source, rows => rows.Any(x => SinkHistory.VersionOf(x) == newest));
            await PastHeadAsync(source, storage, logs, history.Head);
            Assert.True(logs.Errors.Count == 0, string.Join(Environment.NewLine, logs.Errors));
            return rows;
        }

        [DuckDbExtensionFact]
        public async Task DuckDbReadsEveryRetainedVersion()
        {
            foreach (var (cdc, reshaped) in new[] { (false, false), (true, false), (false, true), (true, true) })
            {
                var history = await _histories.GetAsync(cdc, reshaped);
                using var full = Files.Of.LocalDisk(history.Full);
                using var connection = new DuckDBConnection("DataSource=:memory:;autoinstall_known_extensions=false;autoload_known_extensions=false");
                connection.Open();
                using var command = connection.CreateCommand();
                lock (DeltaTestKit.DuckDbInstallLock)
                {
                    command.CommandText = $"SET extension_directory = '{DuckDbTimeTravelProbe.ExtensionDirectory.Replace("\\", "/")}'; LOAD delta;";
                    command.ExecuteNonQuery();
                }
                foreach (var (alias, root) in new[] { ("advanced", history.Live), ("mirrored", history.Full) })
                {
                    command.CommandText = $"ATTACH '{Path.GetFullPath(Path.Combine(root, "t")).Replace("\\", "/")}' AS {alias} (TYPE delta)";
                    command.ExecuteNonQuery();
                }
                // Every column's name and type, then every row
                (List<string> Schema, List<string> Rows) Read(string alias, long version)
                {
                    command.CommandText = $"SELECT * FROM {alias} AT (VERSION => {version}) ORDER BY ALL";
                    using var reader = command.ExecuteReader();
                    var schema = Enumerable.Range(0, reader.FieldCount).Select(x => $"{reader.GetName(x)} {reader.GetDataTypeName(x)}").ToList();
                    var rows = new List<string>();
                    while (reader.Read())
                    {
                        rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(x => reader.IsDBNull(x) ? "null" : Convert.ToString(reader.GetValue(x), CultureInfo.InvariantCulture))));
                    }
                    return (schema, rows);
                }

                for (var version = history.C; version <= history.Head; version++)
                {
                    var expected = Read("mirrored", version);
                    var read = Read("advanced", version);
                    Assert.Equal(expected.Schema, read.Schema);
                    Assert.Equal(expected.Rows, read.Rows);
                    // The version's own columns and row count, as our reader reads the full history
                    var columns = (await DeltaTransactionReader.ReadTable(full, "t", version))!.Schema.Fields.Select(x => x.Name);
                    Assert.Equal(columns, read.Schema.Select(x => x.Split(' ')[0]));
                    Assert.Equal((await RetainedRows.RowsAtAsync(full, version)).Rows.Count, read.Rows.Count);
                    _output.WriteLine($"change data {(cdc ? "on" : "off")}{(reshaped ? ", reshaped" : "")}: version {version} reads {read.Rows.Count} rows of ({string.Join(", ", read.Schema)}) through DuckDB on the advanced table, as on the full history");
                }
            }
        }
    }
}
