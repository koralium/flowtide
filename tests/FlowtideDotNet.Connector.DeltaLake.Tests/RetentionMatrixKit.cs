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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using System.Collections.Concurrent;
using Xunit.Sdk;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // One version as our reader rebuilds it, tombstones excluded
    internal sealed record VersionSnapshot(long Version, string Adds, string Schema, string Protocol, string Metadata, string Transactions, string Domains)
    {
        public static VersionSnapshot Of(DeltaTable table)
        {
            var protocol = table.Protocol;
            var metadata = table.Metadata;
            return new VersionSnapshot(
                table.Version,
                Lines(table.AddFiles.Select(x => $"{x.Path}|{x.Size}|{Pairs(x.PartitionValues)}|{Vector(x.DeletionVector)}")),
                metadata.SchemaString ?? "",
                $"{protocol.MinReaderVersion}/{protocol.MinWriterVersion} r[{Features(protocol.ReaderFeatures)}] w[{Features(protocol.WriterFeatures)}]",
                $"{metadata.Id}|{metadata.Name}|{metadata.Description}|{metadata.Format?.Provider}|{Pairs(metadata.Format?.Options)}|{string.Join(",", metadata.PartitionColumns ?? new List<string>())}|{metadata.CreatedTime}|{Pairs(metadata.Configuration)}",
                Lines(table.Transactions.Select(x => $"{x.Key}={x.Value.Version}")),
                Lines(table.DomainMetadata.Select(x => $"{x.Key}={x.Value.Configuration}")));
        }

        private static string Lines(IEnumerable<string> lines) => string.Join("\n", lines.Order(StringComparer.Ordinal));

        private static string Features(IReadOnlyList<string>? features) => features == null ? "-" : string.Join(",", features.Order(StringComparer.Ordinal));

        private static string Pairs(IReadOnlyDictionary<string, string>? pairs) => pairs == null ? "-" : string.Join(",", pairs.Select(x => $"{x.Key}={x.Value}").Order(StringComparer.Ordinal));

        private static string Vector(DeletionVector? vector) => vector == null ? "-" : $"{vector.StorageType}:{vector.PathOrInlineDv}@{vector.Offset}+{vector.SizeInBytes}#{vector.Cardinality}";
    }

    // Rows at one version through our reader, deletion vectors applied, in ordinal order
    internal sealed record VersionRows(long Version, string Schema, IReadOnlyList<string> Rows);

    // What a cell must keep, read before any delete
    internal sealed record Baseline(long C, long Head, IReadOnlyDictionary<long, VersionSnapshot?> Versions, RetainedReferenceOracle.Result? Oracle, IReadOnlySet<string> Retained,
        IReadOnlyCollection<string> Records);

    internal enum CellEnding
    {
        // The advance's done is published and its teardown finished
        Done,
        // Durable work is left that keeps its authority
        Unresolved
    }

    // A delete one of a cell's runs settled, and whether evidence was invalid by then
    internal readonly record struct SettledDelete(int Run, DeletionTarget Target, DeleteOutcome Outcome, bool AfterInvalidation);

    // What a cell's runs did: their settled deletes, the invalidation point, the newest run's ledger and what escaped
    internal sealed class CellOutcome
    {
        private readonly ExecutionTable _table;
        private readonly ConcurrentQueue<SettledDelete> _settled = new ConcurrentQueue<SettledDelete>();
        private readonly ConcurrentQueue<Exception> _escaped = new ConcurrentQueue<Exception>();
        private int _run = -1;
        private volatile bool _invalidated;

        public CellOutcome(ExecutionTable table)
        {
            _table = table;
        }

        public CellEnding Ending { get; set; } = CellEnding.Done;

        public CapacityLedger? Ledger { get; set; }

        public IReadOnlyList<SettledDelete> Settled => _settled.ToArray();

        public IReadOnlyList<Exception> Escaped => _escaped.ToArray();

        // The in-memory store's removals when evidence became invalid
        public int? RemovalsAtInvalidation { get; private set; }

        // A new run at the matrix's slice size, recording its settled deletes ahead of the cell's own hook
        public DeltaTableCleanup Attach(DeltaTableCleanup cleanup)
        {
            var run = Interlocked.Increment(ref _run);
            cleanup.Settings = cleanup.Settings with { MaxPaths = Invariants.SlicePaths };
            var hooks = cleanup.AdvanceHooksForTests;
            var previous = hooks.Settled;
            hooks.Settled = (target, outcome) =>
            {
                _settled.Enqueue(new SettledDelete(run, target, outcome, _invalidated));
                previous?.Invoke(target, outcome);
            };
            return cleanup;
        }

        // Evidence is invalid from here on
        public void Invalidate()
        {
            RemovalsAtInvalidation = Invariants.Removals(_table);
            _invalidated = true;
        }

        // A run's job or turn; only a requested stop may cancel it
        public async Task<CleanupResult?> RunAsync(Func<Task<CleanupResult>> run, CancellationToken stop = default)
        {
            try
            {
                return await run();
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return null;
            }
            catch (XunitException)
            {
                throw;
            }
            catch (Exception e)
            {
                _escaped.Enqueue(e);
                return null;
            }
        }
    }

    // The seven matrix invariants, captured before a cell and asserted after it
    internal static class Invariants
    {
        // Paths per slice in every matrix run
        public const int SlicePaths = 3;

        // O is commit 0 while it is listed, else the oldest classic checkpoint as a marked root
        public static async Task<Baseline> CaptureAsync(ExecutionTable table, long c, long head)
        {
            Assert.InRange(c, 1, head);
            var view = View(table);
            var versions = new Dictionary<long, VersionSnapshot?>();
            var retained = new HashSet<string>(StringComparer.Ordinal);
            for (long version = 1; version <= head; version++)
            {
                var read = await ReadAsync(view, version);
                Assert.True(version < c || read != null, $"Version {version} at or above C does not read before the cell");
                versions[version] = read == null ? null : VersionSnapshot.Of(read);
                if (read != null && version >= c && table.Root == null)
                {
                    foreach (var add in read.AddFiles)
                    {
                        retained.Add(Uri.UnescapeDataString(add.Path!));
                        if (add.DeletionVector?.StorageType == "u")
                        {
                            retained.Add(add.DeletionVector.GetAbsolutePath()!);
                        }
                    }
                }
            }
            RetainedReferenceOracle.Result? oracle = null;
            if (table.Root != null)
            {
                var log = await DeltaTransactionReader.ListLog(table.Raw, "t");
                var listed = await table.Raw.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}");
                var stages = LogClassifier.Classify(listed).Stages.Select(x => x.Entry.Name);
                var rootAtO = !log.Commits.ContainsKey(0);
                var o = rootAtO ? log.Checkpoints[^1].Version : 0;
                oracle = await RetainedReferenceOracle.ComputeAsync(NativeTable(table), o, rootAtO, false, c, head, stages);
                retained.UnionWith(oracle.Retained);
            }
            var missing = await MissingAsync(table, retained);
            Assert.True(missing.Count == 0, $"The table lacks files its retained versions reference: {string.Join(", ", missing)}");
            return new Baseline(c, head, versions, oracle, retained, Names(await DeltaTransactionReader.ListLog(view, "t")));
        }

        public static async Task AssertAsync(ExecutionTable table, Baseline baseline, CellOutcome outcome)
        {
            var view = View(table);
            var settled = outcome.Settled;
            var c = baseline.C;

            // 1: each retained version reads as before; an older one reads as before or fails, never as anything else
            for (long version = 1; version <= baseline.Head; version++)
            {
                var read = await ReadAsync(view, version);
                var now = read == null ? null : VersionSnapshot.Of(read);
                var before = baseline.Versions[version];
                Check(version >= c ? now == before : now == null || now == before, 1, () => $"version {version} reads {Describe(now)}, before the cell {Describe(before)}");
            }

            // 2: every retained reference exists; the payload deleted is the retired set, all of it once done
            var gone = await MissingAsync(table, baseline.Retained);
            Check(gone.Count == 0, 2, () => $"retained files are gone: {string.Join(", ", gone)}");
            if (baseline.Oracle is { } oracle)
            {
                var retired = oracle.Retired;
                var deleted = settled.Where(x => x.Target.Kind == TargetKind.Payload && x.Outcome.Status == DeleteStatus.Deleted).Select(x => Relative(table, x.Target.Key)).ToHashSet(StringComparer.Ordinal);
                var stray = deleted.Except(retired).ToArray();
                Check(stray.Length == 0, 2, () => $"deleted payload outside the retired set: {string.Join(", ", stray)}");
                if (outcome.Ending == CellEnding.Done)
                {
                    var undeleted = retired.Except(deleted).ToArray();
                    Check(undeleted.Length == 0, 2, () => $"retired payload this advance never deleted: {string.Join(", ", undeleted)}");
                    var present = retired.Except(await MissingAsync(table, retired)).ToArray();
                    Check(present.Length == 0, 2, () => $"retired payload still present: {string.Join(", ", present)}");
                }
            }

            // 3: nothing is deleted once evidence is invalid
            var invalid = settled.Where(x => x.AfterInvalidation && x.Outcome.Status == DeleteStatus.Deleted).Select(x => x.Target.Key).ToArray();
            Check(invalid.Length == 0, 3, () => $"deleted after evidence was invalid: {string.Join(", ", invalid)}");
            if (outcome.RemovalsAtInvalidation is { } removals && table.Hooks != null)
            {
                var after = Removals(table);
                Check(after == removals, 3, () => $"{after - removals} removals after evidence was invalid");
            }

            var listing = await DeltaTransactionReader.ListLog(view, "t");
            var floor = ControlFence.Of(listing).ReservedFloor;
            var controls = Names(listing);
            if (outcome.Ending == CellEnding.Unresolved)
            {
                // 4: unresolved work keeps its reservation, its demand and its records but a replaced progress
                Check(floor == c, 4, () => $"the floor is {floor}, not C {c}");
                var ledger = outcome.Ledger;
                Check(ledger != null && (ledger.Current != default || ledger.Total(CapacityState.Paused) != default || ledger.Total(CapacityState.Suspended) != default), 4,
                    () => "the ledger holds no demand");
                var lost = baseline.Records.Except(controls).Where(x => !Replaced(x, controls)).ToArray();
                Check(lost.Length == 0, 4, () => $"records are gone: {string.Join(", ", lost)}");
            }
            else
            {
                // 6: the teardown leaves only the done at C and no reservation
                Check(floor == ControlFence.NoReservation, 6, () => $"the floor is {floor} after the teardown");
                Check(controls.Length == 1, 6, () => $"{controls.Length} records after the teardown: {string.Join(", ", controls)}");
                Check(ControlName.TryParse(controls[0], out var done) && done.Kind == ControlKind.Done && done.Boundary == c, 6, () => $"{controls[0]} is not a done at C {c}");
            }

            // 5: each restart repeats at most one slice of payload and one log delete
            foreach (var run in settled.Select(x => x.Run).Distinct().Where(x => x > 0))
            {
                foreach (var (kind, bound) in new[] { (TargetKind.Payload, SlicePaths), (TargetKind.Log, 1) })
                {
                    var earlier = settled.Where(x => x.Run < run && x.Target.Kind == kind && x.Outcome.Done).Select(x => x.Target.Key).ToHashSet(StringComparer.Ordinal);
                    var repeated = settled.Where(x => x.Run == run && x.Target.Kind == kind && earlier.Contains(x.Target.Key)).Select(x => x.Target.Key).Distinct().ToArray();
                    Check(repeated.Length <= bound, 5, () => $"run {run} repeated {repeated.Length} {kind} deletes: {string.Join(", ", repeated)}");
                }
            }

            // 7: nothing escaped, and what was kept and never deleted later is still there
            Check(outcome.Escaped.Count == 0, 7, () => $"escaped: {string.Join("; ", outcome.Escaped.Select(x => $"{x.GetType().Name}: {x.Message}"))}");
            for (int i = 0; i < settled.Count; i++)
            {
                var kept = settled[i];
                if (kept.Outcome.Status != DeleteStatus.Kept || settled.Skip(i + 1).Any(x => x.Target.Key == kept.Target.Key && x.Outcome.Done))
                {
                    continue;
                }
                var native = table.Root != null ? NativePath(table, kept.Target.Key) : null;
                var exists = native != null ? File.Exists(native) || Directory.Exists(native) : await view.Exists(kept.Target.Key);
                Check(exists, 7, () => $"{kept.Target.Key}, kept for {kept.Outcome.Reason}, is gone");
            }
        }

        // A native key as given, a store-form key under the local root
        private static string NativePath(ExecutionTable table, string key)
        {
            var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
            return key.StartsWith(NativeTable(table) + Path.DirectorySeparatorChar, comparison) ? key : Path.GetFullPath(Path.Combine(table.Root!, key.TrimStart('/')));
        }

        // The in-memory store's removals so far
        public static int Removals(ExecutionTable table) => table.Hooks?.Requests.Count(x => x.StartsWith("Rm ", StringComparison.Ordinal)) ?? 0;

        // A table-relative name of a delete's key, upper-cased on local disk as the oracle names it
        public static string Relative(ExecutionTable table, string key)
        {
            if (table.Root == null)
            {
                return key.StartsWith("/t/", StringComparison.Ordinal) ? key.Substring(3) : key;
            }
            var native = NativeTable(table) + Path.DirectorySeparatorChar;
            var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
            return RetainedReferenceOracle.Upper((key.StartsWith(native, comparison) ? key.Substring(native.Length) : key).Replace('\\', '/'));
        }

        public static string NativeTable(ExecutionTable table) => Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.Combine(table.Root!, "t")));

        // Reads with the in-memory store's copied reads and none of the cell's hooks or hidden paths
        public static IFileStorage View(ExecutionTable table) => table.Hooks != null ? new HookFileStorage(table.Hooks.Inner) : table.Raw;

        private static async Task<DeltaTable?> ReadAsync(IFileStorage view, long version)
        {
            try
            {
                return await DeltaTransactionReader.ReadTable(view, "t", version);
            }
            catch (Exception)
            {
                return null;
            }
        }

        // Names not on storage
        private static async Task<List<string>> MissingAsync(ExecutionTable table, IEnumerable<string> names)
        {
            var missing = new List<string>();
            if (table.Root != null)
            {
                var native = NativeTable(table);
                var log = Path.Combine(native, "_delta_log") + Path.DirectorySeparatorChar;
                var existing = Directory.EnumerateFiles(native, "*", SearchOption.AllDirectories).Where(x => !x.StartsWith(log, StringComparison.Ordinal))
                    .Select(x => RetainedReferenceOracle.Upper(Path.GetRelativePath(native, x).Replace('\\', '/'))).ToHashSet(StringComparer.Ordinal);
                missing.AddRange(names.Where(x => !existing.Contains(x)));
                return missing;
            }
            var view = View(table);
            foreach (var name in names)
            {
                if (!await view.Exists($"/t/{name}"))
                {
                    missing.Add(name);
                }
            }
            return missing;
        }

        // A progress record its successor replaced
        private static bool Replaced(string name, IReadOnlyCollection<string> controls)
        {
            return ControlName.TryParse(name, out var removed) && removed.Kind == ControlKind.Progress && controls.Any(x => ControlName.TryParse(x, out var now) && now.Kind == ControlKind.Progress
                && now.Boundary == removed.Boundary && now.AdvanceId == removed.AdvanceId && now.Digest == removed.Digest && now.Ordinal > removed.Ordinal);
        }

        private static string[] Names(DeltaTransactionReader.LogListing listing) => listing.Controls.Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();

        private static string Describe(VersionSnapshot? snapshot) => snapshot == null ? "nothing" : snapshot.ToString();

        private static void Check(bool holds, int invariant, Func<string> detail)
        {
            if (!holds)
            {
                throw new XunitException($"Invariant {invariant}: {detail()}");
            }
        }
    }

    // Attempt one of the kind's first record fails and lands later: a release plants the bytes another attempt published under its name, torn if asked
    internal sealed class LandsLater
    {
        private readonly ExecutionTable _table;
        private readonly ControlKind _kind;
        private readonly bool _torn;
        private byte[]? _bytes;

        public LandsLater(ExecutionTable table, ControlKind kind, bool torn = false)
        {
            _table = table;
            _kind = kind;
            _torn = torn;
        }

        // Attempt one's name, unwritten until the release
        public string? First { get; private set; }

        // The attempt that published the record
        public string? Published { get; private set; }

        // The cleanup's BeforePublish, or awaited inside it
        public async Task Hook(ControlKind kind, string name)
        {
            // The published record's bytes, at the next publication
            if (Published != null && _bytes == null)
            {
                _bytes = await ReadAsync(Published);
            }
            if (kind != _kind)
            {
                return;
            }
            if (First == null)
            {
                First = name;
                throw new IOException($"Attempt one of {name} lands later");
            }
            if (_bytes == null)
            {
                Published = name;
            }
        }

        // Plants the published bytes under attempt one's name and returns it
        public async Task<string> ReleaseAsync()
        {
            Assert.True(First != null, $"No {_kind} record was published");
            if (_bytes == null && Published != null)
            {
                _bytes = await ReadAsync(Published);
            }
            Assert.True(_bytes != null, $"No attempt after {First} published");
            await CleanupRecordKit.PlantAsync(_table.Raw, "t", First!, _torn ? CleanupRecordKit.Truncate(_bytes!, _bytes!.Length - 1) : _bytes!);
            return First!;
        }

        private async Task<byte[]?> ReadAsync(string name)
        {
            using var stream = await Invariants.View(_table).OpenRead(_table.LogPath(name));
            if (stream == null)
            {
                return null;
            }
            using var copy = new MemoryStream();
            await stream.CopyToAsync(copy);
            return copy.ToArray();
        }
    }

    // Rows and schema at a version through our readers, of every column of the version or of the given ones
    internal static class RetainedRows
    {
        public static async Task<VersionRows> RowsAtAsync(IFileStorage storage, long version, IReadOnlyList<string>? columns = null)
        {
            var table = await DeltaTransactionReader.ReadTable(storage, "t", version) ?? throw new InvalidOperationException("The table has no log");
            columns ??= table.Schema.Fields.Select(x => x.Name).ToList();
            var reader = new ParquetSharpReader();
            reader.Initialize(table, columns);
            var rows = new List<string>();
            foreach (var add in table.AddFiles)
            {
                IDeleteVector vector = add.DeletionVector == null ? EmptyDeleteVector.Instance : await DeletionVectorReader.ReadDeletionVector(storage, "t", add.DeletionVector);
                await foreach (var batch in reader.ReadDataFile(storage, "t", add.Path!, vector, add.PartitionValues, GlobalMemoryManager.Instance))
                {
                    try
                    {
                        for (int row = 0; row < batch.count; row++)
                        {
                            rows.Add(string.Join("|", Enumerable.Range(0, columns.Count).Select(x => batch.data.Columns[x].GetValueAt(row, default).ToString())));
                        }
                    }
                    finally
                    {
                        batch.data.Dispose();
                    }
                }
            }
            rows.Sort(StringComparer.Ordinal);
            return new VersionRows(table.Version, table.Metadata.SchemaString ?? "", rows);
        }
    }

    // A churned table with payload files, built once per class and cloned per cell before planning: a directory copy on local disk, an entry copy in memory
    public sealed class MatrixTemplate : IDisposable
    {
        private readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();
        private readonly Lazy<Task<ExecutionTable>> _local;
        private readonly Lazy<Task<ExecutionTable>> _memory;

        public MatrixTemplate()
        {
            _local = new Lazy<Task<ExecutionTable>>(() => BuildAsync(true));
            _memory = new Lazy<Task<ExecutionTable>>(() => BuildAsync(false));
        }

        public void Dispose()
        {
            _kit.Dispose();
        }

        private async Task<ExecutionTable> BuildAsync(bool local)
        {
            var table = await _kit.ChurnedAsync(local);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            return table;
        }

        // A copy owned by the cell's kit
        internal async Task<ExecutionTable> CloneAsync(AdvanceExecutionKit kit, bool local = true)
        {
            var template = await (local ? _local : _memory).Value;
            if (local)
            {
                var root = kit.NewRoot();
                foreach (var file in Directory.EnumerateFiles(template.Root!, "*", SearchOption.AllDirectories))
                {
                    var target = Path.Combine(root, Path.GetRelativePath(template.Root!, file));
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    File.Copy(file, target);
                }
                return kit.Open(root, template.Head);
            }
            var clone = kit.Open(null, template.Head);
            var source = Invariants.View(template);
            foreach (var entry in await source.Ls("/t/", recurse: true))
            {
                if (entry.Path.IsFolder)
                {
                    continue;
                }
                using var read = await source.OpenRead(entry.Path);
                await using var write = await clone.Raw.OpenWrite(entry.Path);
                await read!.CopyToAsync(write);
            }
            clone.Hooks!.ClearRequests();
            return clone;
        }
    }
}
