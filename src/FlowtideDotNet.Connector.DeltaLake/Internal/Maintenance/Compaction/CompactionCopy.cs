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


using Apache.Arrow.Memory;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using ParquetSharp;
using Stowage;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Hashing;
using System.Text.Json;
using DeltaStructType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.StructType;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction
{
    // One input of a bin, as its add action lists it
    internal sealed record CompactionInput(string Path, long Size, long Rows, DeletionVector? DeletionVector);

    // Cold files and the output they fold into
    internal sealed class CompactionBin
    {
        public required DeltaStructType Table { get; init; }

        public required string TableName { get; init; }

        public bool ColumnsById { get; init; }

        public required DeltaParquetCodec Codec { get; init; }

        // T_eff
        public required long TargetBytes { get; init; }

        // Output bytes over input live bytes, as the bin builder learned it
        public double Expansion { get; init; } = 1;

        public required IReadOnlyList<CompactionInput> Inputs { get; init; }

        // A, the overshoot the output may have
        public long Overshoot => Math.Min(4L << 20, TargetBytes / 4);

        // F, the builder bytes that close a row group
        public long FlushBytes => FlushBytesFor(TargetBytes);

        // T/4 within 1 to 8 MiB, so larger targets add groups
        public static long FlushBytesFor(long target) => Math.Min(8L << 20, Math.Max(1L << 20, target / 4));
    }

    internal sealed class CompactionContext
    {
        private ICheckpointTransport? _transport;

        public required IFileStorage Storage { get; init; }

        public required IOPath Table { get; init; }

        public required JobScratch Scratch { get; init; }

        public required JobMemory Memory { get; init; }

        public ICheckpointTransport Transport
        {
            get => _transport ??= new StowageCheckpointTransport(Storage, Table);
            init => _transport = value;
        }

        public TransferAllowance? Transfer { get; init; }

        public TimeProvider Clock { get; init; } = TimeProvider.System;

        public DateTimeOffset? Deadline { get; init; }

        public CompactionLimits Limits { get; init; } = CompactionLimits.Default;

        public bool Publish { get; init; } = true;

        public Func<string, long>? FreeSpaceForTests { get; init; }

        // Fires at "copy", "flush", "close", "move" and "sync"
        public Action<string>? FaultForTests { get; init; }
    }

    internal enum CompactionStatus
    {
        Copied,
        Declined,
        Abandoned
    }

    // An input left out; alone means the input failed by itself, so the caller memoizes it
    internal readonly record struct InputDecline(int Input, CompactionReason Reason, bool Alone, string Detail);

    internal readonly record struct CopiedInput(int Input, string Path, string? DeletionVectorId, long Rows);

    internal sealed class CompactionResult
    {
        public CompactionStatus Status { get; set; }

        // An abandon's reason; null for the size and row caps, the deadline and checks
        public CompactionReason? Reason { get; set; }

        public string Detail { get; set; } = "";

        public List<InputDecline> Declines { get; } = new List<InputDecline>();

        public List<CopiedInput> Copied { get; } = new List<CopiedInput>();

        public DeltaAddAction? Output { get; set; }

        // The output in scratch when not published, for tests
        public string? LocalOutput { get; set; }

        public long DeletionVectorReads { get; set; }

        public long BytesRead { get; set; }

        public long BytesWritten { get; set; }

        // Output bytes thrown away by an abandon
        public long BytesWasted { get; set; }

        public long Requests { get; set; }

        public long NativeOpens { get; set; }

        public long ManagedPeak { get; set; }

        public long NativePeak { get; set; }

        // Output bytes over the copied inputs' live bytes
        public double Expansion { get; set; }
    }

    // Copies a bin into one output: admit every input from its footer, then copy (plan §4.3.6)
    internal sealed class CompactionCopy
    {
        // A run container's decode holds up to this much beside the vector
        private const long VectorTransient = 400L << 10;

        // A footer chunk at worst: two 4 KiB statistics values and the rest
        private const long FooterBytesPerChunk = 9L << 10;

        // Larger output groups re-admit only at smaller batches
        private const long GroupCeiling = 12L << 20;

        private readonly CompactionContext _context;
        private readonly CompactionBin _bin;
        private readonly JobMemory _memory;
        private readonly CompactionResult _result = new CompactionResult();
        private readonly OutputShape _output;
        private readonly AdmissionSettings _settings;
        private readonly List<Admitted> _admitted = new List<Admitted>();
        private readonly Dictionary<string, LocalFile> _vectorFiles = new Dictionary<string, LocalFile>(StringComparer.Ordinal);
        // A container that failed fails every input sharing it
        private readonly Dictionary<string, (CompactionReason Reason, bool Alone, string Detail)> _vectorFailures = new Dictionary<string, (CompactionReason, bool, string)>(StringComparer.Ordinal);
        private long _vectorBytes;
        private long[] _levelCaps = System.Array.Empty<long>();
        private readonly long _cap;
        // The output's disk, held from before pass 1 until pass 2 takes it
        private long _outputCharge;

        private CompactionCopy(CompactionContext context, CompactionBin bin)
        {
            _context = context;
            _bin = bin;
            _memory = context.Memory;
            _output = new OutputShape(bin.Table);
            _settings = new AdmissionSettings(bin.FlushBytes, bin.Codec.Compression) { NativeCap = _memory.NativeCap, ManagedCap = _memory.ManagedCap };
            _cap = checked(bin.TargetBytes + bin.Overshoot);
        }

        public static async Task<CompactionResult> RunAsync(CompactionContext context, CompactionBin bin, CancellationToken cancellationToken)
        {
            var copy = new CompactionCopy(context, bin);
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                // The output's disk first, so a freeze in pass 1 leaves room for it
                context.Scratch.Ledger.Reserve(copy._cap);
                copy._outputCharge = copy._cap;
                await copy.AdmitAsync(cancellationToken);
                if (copy._admitted.Count < 2)
                {
                    copy._result.Status = CompactionStatus.Declined;
                    copy._result.Detail = $"{copy._admitted.Count} input(s) admitted, a bin needs two";
                }
                else
                {
                    await copy.CopyAsync(cancellationToken);
                }
            }
            finally
            {
                copy.Cleanup();
            }
            return copy._result;
        }

        // A local copy of an input or vector file; owned ones live in scratch
        private sealed record LocalFile(string Path, bool Owned, long Length);

        private sealed class Admitted
        {
            public required int Index { get; init; }

            public required CompactionInput Input { get; init; }

            public required LocalFile File { get; init; }

            public required InputSummary Summary { get; init; }

            public required long FooterLength { get; init; }

            public required long[] GroupStarts { get; init; }

            public required IReadOnlyList<Apache.Arrow.Types.TimeUnit> Units { get; init; }

            public required IReadOnlyList<PrecisionLeaf> Precision { get; init; }

            public required IReadOnlyList<string> MissingPaths { get; init; }

            public LocalFile? Vector { get; init; }

            // Summary bytes still held
            public long Held { get; set; }
        }

        // Pass 1: fetch, scan and admit each input, keeping a running prefix (R26)
        private async Task AdmitAsync(CancellationToken cancellationToken)
        {
            long rows = 0;
            for (int i = 0; i < _bin.Inputs.Count && _admitted.Count < _context.Limits.MaxInputs; i++)
            {
                var input = _bin.Inputs[i];
                var live = input.Rows - (input.DeletionVector?.Cardinality ?? 0);
                if (rows + live > _context.Limits.MaxRows)
                {
                    break;
                }
                cancellationToken.ThrowIfCancellationRequested();
                var stop = await AdmitOneAsync(i, input, cancellationToken);
                if (stop)
                {
                    break;
                }
                if (_admitted.Count > 0 && _admitted[^1].Index == i)
                {
                    rows += live;
                }
            }
        }

        // True when the prefix is frozen and no later input is fetched
        private async Task<bool> AdmitOneAsync(int index, CompactionInput input, CancellationToken cancellationToken)
        {
            var vector = input.DeletionVector;
            if (vector != null && vector.StorageType != "u" && vector.StorageType != "p")
            {
                Decline(index, vector.StorageType == "i" ? CompactionReason.InlineDv : CompactionReason.Malformed, true, $"A deletion vector of storage type {vector.StorageType}");
                return false;
            }
            LocalFile? vectorFile = null;
            LocalFile? file;
            try
            {
                // The vector first, so a declined one costs no data read
                if (vector != null)
                {
                    var (local, reason, alone, detail) = await FetchVectorAsync(vector, cancellationToken);
                    if (local == null)
                    {
                        Decline(index, reason!.Value, alone, detail);
                        return false;
                    }
                    vectorFile = local;
                }
                file = await FetchAsync(_context.Table.Combine(input.Path), input.Size, cancellationToken);
            }
            catch (CheckpointDeclinedException e) when (e.Reason == MaintenanceReason.DiskCap)
            {
                // Scratch is full for now, so freeze what we have
                Decline(index, CompactionReason.ManagedBound, false, e.Message);
                return true;
            }
            if (file == null)
            {
                Decline(index, CompactionReason.Malformed, false, $"Input {input.Path} is gone or changed size");
                return false;
            }
            try
            {
                var (scan, plan) = DataFileAdmission.Preflight(file.Path, _bin.Table, _bin.ColumnsById, _memory);
                try
                {
                    var summary = DataFileAdmission.Summarize(scan, plan, _output);
                    var admission = DataFileAdmission.Admit(_admitted.Select(x => x.Summary).ToList(), summary, _output, _settings);
                    if (admission.Reason != null)
                    {
                        DeleteLocal(file);
                        Decline(index, admission.Reason.Value, !admission.PrefixDrop, admission.Detail);
                        return admission.PrefixDrop;
                    }
                    summary.Candidate = admission.Candidate;
                    DataFileAdmission.KeepGroupChecks(summary, scan, plan, _output);
                    _memory.Reserve(summary.Bytes);
                    var starts = new long[scan.Footer.RowGroupRows.Count];
                    for (int g = 1; g < starts.Length; g++)
                    {
                        starts[g] = starts[g - 1] + scan.Footer.RowGroupRows[g - 1];
                    }
                    _admitted.Add(new Admitted()
                    {
                        Index = index,
                        Input = input,
                        File = file,
                        Summary = summary,
                        Held = summary.Bytes,
                        FooterLength = scan.Footer.FooterLength,
                        GroupStarts = starts,
                        Units = plan.Units.ToArray(),
                        Precision = plan.Precision.ToArray(),
                        MissingPaths = plan.MissingPaths.ToArray(),
                        Vector = vectorFile
                    });
                    return false;
                }
                finally
                {
                    _memory.Release(scan.ChargedBytes);
                }
            }
            catch (DataFileDeclinedException e)
            {
                DeleteLocal(file);
                Decline(index, e.Reason, true, e.Message);
                return false;
            }
            catch
            {
                DeleteLocal(file);
                throw;
            }
        }

        // Local disk is read in place, an object store downloads to scratch
        private async Task<LocalFile?> FetchAsync(IOPath path, long? size, CancellationToken cancellationToken)
        {
            var transport = _context.Transport;
            _result.Requests++;
            if (transport.NativePath(path) is { } native)
            {
                await transport.ChargeNativeAsync(1, 0, cancellationToken);
                var info = new FileInfo(native);
                if (!info.Exists || (size != null && info.Length != size))
                {
                    return null;
                }
                await transport.ChargeNativeAsync(0, info.Length, cancellationToken);
                _result.BytesRead += info.Length;
                return new LocalFile(native, false, info.Length);
            }
            if (size > _context.Scratch.Ledger.Cap)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.DiskCap, DeclineDependency.None, $"{path} is larger than the disk cap");
            }
            var download = _context.Scratch.NewPath(".parquet");
            long length;
            try
            {
                using (_context.Transfer != null ? await _context.Transfer.AdmitAsync(size ?? 0, cancellationToken) : null)
                {
                    await using var target = _context.Scratch.Create(download);
                    length = await transport.DownloadAsync(path, target, _memory, cancellationToken);
                }
            }
            catch
            {
                _result.BytesRead += Partial(download);
                _context.Scratch.Delete(download);
                throw;
            }
            if (length < 0 || (size != null && length != size))
            {
                _context.Scratch.Delete(download);
                return null;
            }
            _result.BytesRead += length;
            return new LocalFile(download, true, length);
        }

        // Each container once per job, within the vector allowance
        private async Task<(LocalFile? File, CompactionReason? Reason, bool Alone, string Detail)> FetchVectorAsync(DeletionVector vector, CancellationToken cancellationToken)
        {
            string? relative;
            try
            {
                relative = vector.AbsolutePath;
            }
            catch (Exception e) when (e is FormatException or ArgumentOutOfRangeException or NullReferenceException)
            {
                return (null, CompactionReason.Malformed, true, $"A deletion vector path cannot be decoded: {e.Message}");
            }
            // An absolute URI or a dot segment does not resolve under the table
            if (relative == null || relative.Contains("://", StringComparison.Ordinal) || PathCanonicalizer.CheckRelative(relative) != PathRejection.None)
            {
                return (null, CompactionReason.Malformed, true, $"A deletion vector at {relative} is outside the table");
            }
            if (_vectorFiles.TryGetValue(relative, out var known))
            {
                return (known, null, false, "");
            }
            if (_vectorFailures.TryGetValue(relative, out var failed))
            {
                return (null, failed.Reason, failed.Alone, failed.Detail);
            }
            var room = _context.Limits.DeletionVectorBytes - _vectorBytes;
            // With nothing used yet, a container past the room fails by itself
            var whole = _vectorBytes == 0;
            var over = $"The deletion vectors pass the {_context.Limits.DeletionVectorBytes} byte allowance";
            if (room <= 0)
            {
                return Failed(relative, CompactionReason.DvAllowance, false, over);
            }
            LocalFile? file;
            try
            {
                file = await FetchCappedAsync(_context.Table.Combine(relative), room, cancellationToken);
            }
            catch (VectorAllowanceException e)
            {
                // A stopped download's bytes were still read
                _vectorBytes += e.Read;
                _result.BytesRead += e.Read;
                return Failed(relative, CompactionReason.DvAllowance, whole || e.Length > _context.Limits.DeletionVectorBytes, over);
            }
            if (file == null)
            {
                return Failed(relative, CompactionReason.Malformed, true, $"The deletion vector file {relative} is gone");
            }
            _vectorBytes += file.Length;
            _result.DeletionVectorReads++;
            _vectorFiles.Add(relative, file);
            return (file, null, false, "");
        }

        private (LocalFile?, CompactionReason?, bool, string) Failed(string relative, CompactionReason reason, bool alone, string detail)
        {
            _vectorFailures[relative] = (reason, alone, detail);
            return (null, reason, alone, detail);
        }

        // A container has no listed size, so its download stops at the room left
        private async Task<LocalFile?> FetchCappedAsync(IOPath path, long room, CancellationToken cancellationToken)
        {
            var transport = _context.Transport;
            if (transport.NativePath(path) is { } native)
            {
                // Read in place: one charged probe decides the room and the length
                _result.Requests++;
                await transport.ChargeNativeAsync(1, 0, cancellationToken);
                var info = new FileInfo(native);
                if (!info.Exists)
                {
                    return null;
                }
                if (info.Length > room)
                {
                    throw new VectorAllowanceException(0, info.Length);
                }
                await transport.ChargeNativeAsync(0, info.Length, cancellationToken);
                _result.BytesRead += info.Length;
                return new LocalFile(native, false, info.Length);
            }
            _result.Requests++;
            var download = _context.Scratch.NewPath(".bin");
            long length;
            try
            {
                using (_context.Transfer != null ? await _context.Transfer.AdmitAsync(room, cancellationToken) : null)
                {
                    await using var target = new CappedStream(_context.Scratch.Create(download), room);
                    length = await transport.DownloadAsync(path, target, _memory, cancellationToken, room);
                }
                // Reads stop at the room, so a container that fills it may be larger
                if (length >= room)
                {
                    throw new VectorAllowanceException(length);
                }
            }
            catch (Exception e)
            {
                // A stopped download counts its own bytes
                _result.BytesRead += e is VectorAllowanceException ? 0 : Partial(download);
                _context.Scratch.Delete(download);
                throw;
            }
            if (length < 0)
            {
                _context.Scratch.Delete(download);
                return null;
            }
            _result.BytesRead += length;
            return new LocalFile(download, true, length);
        }

        // Pass 2: freeze the output over the prefix, then copy each input
        private async Task CopyAsync(CancellationToken cancellationToken)
        {
            var summaries = _admitted.Select(x => x.Summary).ToList();
            var shared = DataFileAdmission.Freeze(summaries, _output, _settings);
            _levelCaps = DataFileAdmission.LevelCaps(summaries, _output, _settings);
            var units = ParquetSharpWriter.FoldUnits(_admitted.Select(x => x.Units).ToList());
            var first = DataFileAdmission.Terms(summaries[0], summaries[0].Candidate, shared, summaries, _output, _settings);
            // The builders' growth pool and the accumulated statistics, for the whole output (plan §4.3.7)
            var held = checked(shared.Cap + shared.Cap / 2 + first.Statistics);
            _memory.Reserve(held);
            var outputPath = _context.Scratch.NewPath(_bin.Codec.FileExtension);
            var cap = _cap;
            var ledger = _context.Scratch.Ledger;
            long charged = 0;
            InputTerms current = default;
            ParquetSharpWriter? writer = null;
            var opened = false;
            try
            {
                charged = _outputCharge;
                _outputCharge = 0;
                writer = new ParquetSharpWriter(_bin.Table, _bin.Table.Fields.Select(x => x.Name).ToList(), writerProperties: DeltaParquetCompression.GetOutputWriterProperties(_bin.Codec.Compression), units: units);
                writer.NewBatch();
                // The file so far, the group at worst, the page index and the footer
                long index = 0, previous = 0;
                // Arrow writes a group's last column chunk with the next group, so the previous one counts too
                void Cover(BuilderSizes sizes, long rows, int groups)
                {
                    _context.FaultForTests?.Invoke("flush");
                    var (group, pages) = ProjectedGroupBytes(sizes, rows);
                    var needed = checked(new FileInfo(outputPath).Length + previous + group + index + pages + (1L << 20) + FooterBytesPerChunk * groups * _output.Leaves.Count);
                    if (needed > charged)
                    {
                        ledger.Reserve(needed - charged);
                        charged = needed;
                    }
                    index += pages;
                    previous = group;
                }
                var budget = new GroupBudget(_memory, checked(shared.Built + shared.Writer), checked(NativeConstants.WriterChunkMetadata * _output.Leaves.Count), shared.Cap + shared.Cap / 2, DataFileAdmission.FlushWriter(_output, _bin.Codec.Compression)) { CoverDisk = Cover };
                double liveBytes = 0;
                InputTerms last = default;
                foreach (var admitted in _admitted.ToList())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var input = admitted.Input;
                    var liveShare = input.Rows == 0 ? 0 : 1 - (double)(input.DeletionVector?.Cardinality ?? 0) / input.Rows;
                    // R15: stop at an input boundary before the output passes T_eff + A/2 or 64 groups
                    if (_result.Copied.Count > 0 && (writer.RowGroups >= _context.Limits.SoftGroups || (liveBytes + input.Size * liveShare) * _bin.Expansion > _bin.TargetBytes + _bin.Overshoot / 2.0))
                    {
                        break;
                    }
                    var terms = DataFileAdmission.Terms(admitted.Summary, admitted.Summary.Candidate, shared, summaries, _output, _settings);
                    // The mask, staged synthesis and the vector, beside the builders; R15 stops if it can't be raised
                    var transient = checked(terms.Transient + terms.DeletionVector + VectorTransient);
                    if (!_memory.TryReserve(transient))
                    {
                        break;
                    }
                    bool? outcome;
                    current = terms;
                    try
                    {
                        outcome = await CopyInputAsync(admitted, terms, writer, budget, () =>
                        {
                            if (!opened)
                            {
                                writer.OpenStreaming(outputPath, budget);
                                opened = true;
                            }
                        }, cancellationToken);
                    }
                    finally
                    {
                        _memory.Release(transient);
                    }
                    if (outcome == null)
                    {
                        return;
                    }
                    if (outcome.Value)
                    {
                        liveBytes += input.Size * liveShare;
                        last = terms;
                    }
                    Release(admitted);
                }
                if (_result.Copied.Count == 0)
                {
                    Abandon(null, "0 input(s) copied, an output needs two");
                    return;
                }
                long length;
                // The last group and the footer under the last input's reservation
                using (_memory.ReserveNative(last.NativeCopy))
                {
                    try
                    {
                        if (writer.FlushRowGroup() is { } refused)
                        {
                            Abandon(refused, writer.RefusalDetail ?? "");
                            return;
                        }
                        _context.FaultForTests?.Invoke("close");
                        length = writer.CloseStreaming();
                    }
                    catch (Exception e) when (WriterFailure(e))
                    {
                        Abandon(e is OverflowException ? CompactionReason.BoundOverflow : null, $"Writing the output failed: {e.Message}");
                        return;
                    }
                }
                _result.BytesWritten = length;
                _result.Expansion = liveBytes > 0 ? length / liveBytes : 0;
                // One input copied: nothing uploads, but the planner learns the expansion
                if (_result.Copied.Count < 2)
                {
                    Abandon(null, "1 input(s) copied, an output needs two");
                    return;
                }
                var statistics = JsonSerializer.Serialize(writer.GetStatistics(), StatisticsOptions(_bin.Table));
                var rows = writer.FileRows;
                var groups = writer.RowGroups;
                writer.Dispose();
                writer = null;
                // Disk follows the real length
                if (length > charged)
                {
                    ledger.Reserve(length - charged);
                }
                else
                {
                    ledger.Release(charged - length);
                }
                charged = length;
                _context.Scratch.Adopt(outputPath, charged);
                charged = 0;
                if (length > cap || rows > _context.Limits.MaxRows || groups > _context.Limits.HardGroups)
                {
                    Abandon(groups > _context.Limits.HardGroups ? CompactionReason.GroupCap : null, $"The output has {length} bytes against a cap of {cap}, {rows} rows and {groups} row groups");
                    _context.Scratch.Delete(outputPath);
                    return;
                }
                if (_context.Deadline is { } deadline && _context.Clock.GetUtcNow() > deadline)
                {
                    Abandon(null, "The deadline passed before upload");
                    _context.Scratch.Delete(outputPath);
                    return;
                }
                cancellationToken.ThrowIfCancellationRequested();
                await PublishAsync(outputPath, length, statistics, cancellationToken);
            }
            finally
            {
                try
                {
                    // An open writer closes in Dispose, under the terms the copy held
                    var hold = writer != null && opened && _memory.TryReserveNative(current.NativeCopy) ? current.NativeCopy : 0;
                    try
                    {
                        writer?.Dispose();
                    }
                    finally
                    {
                        _memory.ReleaseNative(hold);
                    }
                    // An unpublished copy stays in scratch until the job ends
                    if (_result.LocalOutput == null && _context.Scratch.Files.Contains(outputPath))
                    {
                        _context.Scratch.Delete(outputPath);
                    }
                    else if (_result.LocalOutput == null && File.Exists(outputPath))
                    {
                        // Groups flushed before an abandon were still written
                        _result.BytesWritten = Math.Max(_result.BytesWritten, new FileInfo(outputPath).Length);
                        // Through scratch, so a failed delete keeps its charge pending
                        _context.Scratch.Adopt(outputPath, charged);
                        charged = 0;
                        _context.Scratch.Delete(outputPath);
                    }
                    if (charged > 0)
                    {
                        ledger.Release(charged);
                    }
                }
                finally
                {
                    _memory.Release(held);
                }
            }
        }

        // A group's pages at worst, and their page index entries
        private (long Group, long Index) ProjectedGroupBytes(BuilderSizes sizes, long rows)
        {
            checked
            {
                long bytes = 0, index = 0;
                for (int l = 0; l < _output.Leaves.Count; l++)
                {
                    var leaf = _output.Leaves[l];
                    var size = sizes.Leaves[l];
                    var values = leaf.Shape switch { LeafShape.Fixed => leaf.Encoded * size.Levels, LeafShape.Bits => (size.Levels + 7) / 8, _ => size.Length + 4 * size.Levels };
                    var plain = values + (leaf.Repeated ? 2 : 1) * size.Levels;
                    var pages = values / DeltaParquetCompression.OutputPageSize + (rows + WriterTerms.RowsPerPage - 1) / WriterTerms.RowsPerPage + 1;
                    bytes += (leaf.Shape == LeafShape.Bits ? 1 : 2) * NativeConstants.Compressed(_bin.Codec.Compression, plain) + 1024 * pages;
                    // A page's min and max, each at most 4 KiB of statistics
                    index += pages * (2 * (leaf.Shape == LeafShape.Bytes ? 4096 : Math.Max(1, leaf.Encoded)) + 64);
                }
                return (bytes, index);
            }
        }

        // True when copied, false when declined alone, null when the bin is abandoned
        private async Task<bool?> CopyInputAsync(Admitted admitted, InputTerms terms, ParquetSharpWriter writer, GroupBudget budget, Action open, CancellationToken cancellationToken)
        {
            var input = admitted.Input;
            var summary = admitted.Summary;
            IDeleteVector deleted = EmptyDeleteVector.Instance;
            if (input.DeletionVector != null)
            {
                try
                {
                    deleted = DecodeVector(input, admitted.Vector!, summary.Rows);
                }
                catch (Exception e) when (e is InvalidDataException or EndOfStreamException or ArgumentOutOfRangeException or OverflowException or IndexOutOfRangeException)
                {
                    Decline(admitted.Index, CompactionReason.Malformed, true, e.Message);
                    return false;
                }
            }
            if (admitted.Precision.Count > 0)
            {
                using (_memory.ReserveNative(terms.NativePrecision))
                {
                    _memory.CountNativeOpen();
                    bool inexact;
                    try
                    {
                        using var reader = ParquetFileReaders.OpenLowLevel(admitted.File.Path, admitted.FooterLength);
                        inexact = TimestampPrecision.HasInexactSurvivor(reader, input.Path, admitted.GroupStarts, admitted.Precision, deleted);
                    }
                    catch (Exception e) when (e is InvalidDataException or ParquetException)
                    {
                        // Nothing is copied yet, so a transient failure only leaves it out
                        Decline(admitted.Index, CompactionReason.Malformed, !Transient(e, admitted.File), e.Message);
                        return false;
                    }
                    if (inexact)
                    {
                        Decline(admitted.Index, CompactionReason.Precision, true, $"Input {input.Path} holds a timestamp a microsecond copy would change");
                        return false;
                    }
                }
            }
            using (_memory.ReserveNative(terms.NativeCopy))
            {
                open();
                budget.ChargedGroups = checked((int)terms.Groups);
                _context.FaultForTests?.Invoke("copy");
                var before = writer.FileRows;
                try
                {
                    if (!await CopyRowsAsync(admitted, writer, deleted, cancellationToken))
                    {
                        return null;
                    }
                }
                catch (InputReadException e)
                {
                    var detail = $"Copying {input.Path} failed: {e.Message}";
                    var overflow = e.InnerException is OverflowException;
                    // A reader failure is the input's own, unless transient
                    if (!Transient(e.InnerException!, admitted.File))
                    {
                        Decline(admitted.Index, overflow ? CompactionReason.BoundOverflow : CompactionReason.Malformed, true, detail);
                    }
                    Abandon(overflow ? CompactionReason.BoundOverflow : null, detail);
                    return null;
                }
                catch (Exception e) when (WriterFailure(e))
                {
                    // The output's writer failed, which no input owns
                    Abandon(e is OverflowException ? CompactionReason.BoundOverflow : null, $"Writing the output at {input.Path} failed: {e.Message}");
                    return null;
                }
                // Rows copied must be the physical rows less the deleted ones
                var copied = writer.FileRows - before;
                if (copied != summary.Rows - deleted.Cardinality)
                {
                    var detail = $"Input {input.Path} gave {copied} rows, {summary.Rows - deleted.Cardinality} expected";
                    Decline(admitted.Index, CompactionReason.Malformed, true, detail);
                    Abandon(null, detail);
                    return null;
                }
                _result.Copied.Add(new CopiedInput(admitted.Index, input.Path, input.DeletionVector?.UniqueId, copied));
                return true;
            }
        }

        // One row group at a time, reserving growth before each batch is read (R18)
        private async Task<bool> CopyRowsAsync(Admitted admitted, ParquetSharpWriter writer, IDeleteVector deleted, CancellationToken cancellationToken)
        {
            var summary = admitted.Summary;
            _memory.CountNativeOpen();
            using var file = Read(() => new ParquetDataFile(admitted.Input.Path, null, ParquetFileReaders.OpenLocal(admitted.File.Path, (int)InputSummary.BatchRows(summary.Candidate), admitted.FooterLength)));
            var binding = Read(() => FileBinding.Create(_bin.Table, file, _bin.TableName, _bin.ColumnsById));
            // The preflight read the same footer, so the bindings and groups agree
            if (!binding.TimestampUnits.SequenceEqual(admitted.Units) || !binding.PrecisionLeaves.SequenceEqual(admitted.Precision) || !binding.MissingPaths.SequenceEqual(admitted.MissingPaths) || !file.RowGroupStarts.SequenceEqual(admitted.GroupStarts))
            {
                Debug.Fail("The preflight and FileBinding disagree");
                Abandon(CompactionReason.BindingPreflight, $"Input {admitted.Input.Path} binds unlike its preflight");
                return false;
            }
            Read(() =>
            {
                file.CheckRowGroups(binding.LeafColumns);
                return true;
            });
            using var cursor = deleted.GetEnumerator();
            var hasDeleted = cursor.MoveNext();
            var mask = new BatchDeleteMask();
            for (int g = 0; g < file.RowGroupStarts.Length; g++)
            {
                // Dense lists also close a group at the level cap admission counts
                if (PastLevelCap(writer, summary, g) && writer.FlushRowGroup() is { } dense)
                {
                    Abandon(dense, writer.RefusalDetail ?? "");
                    return false;
                }
                var index = file.RowGroupStarts[g];
                var end = g + 1 < file.RowGroupStarts.Length ? file.RowGroupStarts[g + 1] : file.NumRows;
                await using var batches = file.ReadRowGroup(g, binding.LeafColumns).GetAsyncEnumerator(cancellationToken);
                // The length after a batch stands until the next one
                long? length = null;
                // No growth is reserved once the group's rows are in
                while (index < end)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var open = length ?? writer.Measure().Length;
                    var rows = Math.Min(InputSummary.BatchRows(summary.Candidate), end - index);
                    if (StartsNextGroup(open, ExpectedGrowth(open, writer.WrittenCount, summary.GroupWhole[g], end - file.RowGroupStarts[g], summary.GroupOut[g], rows), _bin.FlushBytes) && writer.FlushRowGroup() is { } early)
                    {
                        Abandon(early, writer.RefusalDetail ?? "");
                        return false;
                    }
                    if (writer.ReserveGrowth(summary.GroupOut[g]) is { } grow)
                    {
                        Abandon(grow, writer.RefusalDetail ?? "");
                        return false;
                    }
                    if (!await NextBatchAsync(batches))
                    {
                        break;
                    }
                    using (var batch = batches.Current)
                    using (var rebound = Read(() => binding.Rebind(batch, MemoryAllocator.Default.Value)))
                    {
                        Debug.Assert(rebound.OwnedBytes <= summary.GroupSynth[g], "Synthesis stays within Synth_g");
                        mask.Reset(index, rebound.Length);
                        while (hasDeleted && cursor.Current < index + rebound.Length)
                        {
                            mask.Add(cursor.Current);
                            hasDeleted = cursor.MoveNext();
                        }
                        var appended = Read(() => writer.CopyBatch(rebound, index, mask));
                        Debug.Assert(appended <= summary.GroupOut[g], "The builders grow within Out_g");
                        index += rebound.Length;
                    }
                    length = writer.Measure().Length;
                    if (length >= _bin.FlushBytes)
                    {
                        length = null;
                        if (writer.FlushRowGroup() is { } flush)
                        {
                            Abandon(flush, writer.RefusalDetail ?? "");
                            return false;
                        }
                    }
                }
                // A short group would hand its deleted tail to the next
                if (index != end)
                {
                    var detail = $"Input {admitted.Input.Path} gave {index - file.RowGroupStarts[g]} of {end - file.RowGroupStarts[g]} rows in row group {g}";
                    Decline(admitted.Index, CompactionReason.Malformed, true, detail);
                    Abandon(null, detail);
                    return false;
                }
            }
            return true;
        }

        // Arrow's stream reports a page it cannot decode as a bare Exception
        private static async Task<bool> NextBatchAsync(IAsyncEnumerator<Apache.Arrow.RecordBatch> batches)
        {
            try
            {
                return await batches.MoveNextAsync();
            }
            catch (Exception e) when (ReadFailure(e))
            {
                throw new InputReadException(e);
            }
        }

        // A read of the input, its failures marked as the input's
        private static T Read<T>(Func<T> read)
        {
            try
            {
                return read();
            }
            catch (Exception e) when (ReadFailure(e))
            {
                throw new InputReadException(e);
            }
        }

        private static bool ReadFailure(Exception e) => e is InvalidDataException or ParquetException or OverflowException || e.GetType() == typeof(Exception);

        private static bool WriterFailure(Exception e) => e is InvalidDataException or ParquetException or InvalidOperationException or OutOfMemoryException or OverflowException;

        // OS errors, allocation failures and a changed file are transient
        private static bool Transient(Exception e, LocalFile file)
        {
            var message = e.Message;
            if (message.Contains("[errno ", StringComparison.Ordinal) || message.Contains("[Windows error ", StringComparison.Ordinal) || message.Contains("Out of memory", StringComparison.Ordinal))
            {
                return true;
            }
            try
            {
                return new FileInfo(file.Path).Length != file.Length;
            }
            catch (Exception x) when (x is IOException or UnauthorizedAccessException)
            {
                return true;
            }
        }

        // A partial download's bytes, still read from the store
        private static long Partial(string download) => File.Exists(download) ? new FileInfo(download).Length : 0;

        private sealed class InputReadException : Exception
        {
            public InputReadException(Exception inner) : base(inner.Message, inner)
            {
            }
        }

        // Half-full groups close before a batch would pass the ceiling
        internal static bool StartsNextGroup(long open, long expected, long flushBytes) => open >= flushBytes / 2 && open + expected > GroupCeiling;

        // Rows at the open group's or own group's mean, bounded
        internal static long ExpectedGrowth(long open, int written, long groupBytes, long groupRows, long bound, long rows)
        {
            var perRow = Math.Max((double)open / Math.Max(1, written), (double)groupBytes / Math.Max(1, groupRows));
            return (long)Math.Min(bound, perRow * rows);
        }

        // True when this input group would take a list leaf past its cap
        private bool PastLevelCap(ParquetSharpWriter writer, InputSummary summary, int group)
        {
            if (!_levelCaps.Any(x => x > 0))
            {
                return false;
            }
            var leaves = writer.Measure().Leaves;
            return Enumerable.Range(0, _levelCaps.Length).Any(l => _levelCaps[l] > 0 && leaves[l].Levels > 0 && leaves[l].Levels + Math.Min(summary.GroupLevels[group], summary.LeafGroupLevels[l]) > _levelCaps[l]);
        }

        // The vector from its scratch copy, its header checked before any allocation
        private static IDeleteVector DecodeVector(CompactionInput input, LocalFile container, long rows)
        {
            var vector = input.DeletionVector!;
            var offset = vector.Offset ?? 1;
            if (offset < 1 || vector.SizeInBytes < 4 || offset + 8L + vector.SizeInBytes > container.Length)
            {
                throw new InvalidDataException($"The deletion vector of {input.Path} lies outside its file");
            }
            using var stream = new FileStream(container.Path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            // Size, magic, then at most one bitmap at key 0 with a container per 65,536 rows
            Span<byte> head = stackalloc byte[28];
            stream.Position = offset;
            stream.ReadExactly(head.Slice(0, 16));
            var bitmaps = BinaryPrimitives.ReadInt64LittleEndian(head.Slice(8));
            if (BinaryPrimitives.ReadInt32BigEndian(head) != vector.SizeInBytes || BinaryPrimitives.ReadInt32LittleEndian(head.Slice(4)) != 1681511377 || bitmaps < 0 || bitmaps > 1)
            {
                throw new InvalidDataException($"The deletion vector of {input.Path} has a malformed frame");
            }
            if (bitmaps == 1)
            {
                stream.ReadExactly(head.Slice(16, 8));
                var cookie = BinaryPrimitives.ReadUInt32LittleEndian(head.Slice(20));
                if ((cookie & 0xFFFF) != 12347)
                {
                    stream.ReadExactly(head.Slice(24, 4));
                }
                var containers = (cookie & 0xFFFF) == 12347 ? (cookie >> 16) + 1 : BinaryPrimitives.ReadUInt32LittleEndian(head.Slice(24));
                if (BinaryPrimitives.ReadInt32LittleEndian(head.Slice(16)) != 0 || containers > (rows + 65_535) / 65_536)
                {
                    throw new InvalidDataException($"The deletion vector of {input.Path} has a header past its {rows} rows");
                }
            }
            // The checksum, so a damaged vector never drops the wrong rows for good
            stream.Position = offset + 4;
            var crc = new Crc32();
            Span<byte> buffer = stackalloc byte[4096];
            for (long left = vector.SizeInBytes; left > 0;)
            {
                var read = stream.Read(buffer.Slice(0, (int)Math.Min(left, buffer.Length)));
                if (read == 0)
                {
                    throw new EndOfStreamException($"The deletion vector of {input.Path} ends early");
                }
                crc.Append(buffer.Slice(0, read));
                left -= read;
            }
            stream.ReadExactly(head.Slice(0, 4));
            if (crc.GetCurrentHashAsUInt32() != BinaryPrimitives.ReadUInt32BigEndian(head))
            {
                throw new InvalidDataException($"The deletion vector of {input.Path} fails its checksum");
            }
            stream.Position = 0;
            var decoded = DeletionVectorReader.ReadVector(stream, vector.Offset);
            // The bitmap ends where its checked payload does, never in the checksum
            if (stream.Position != offset + 4 + vector.SizeInBytes)
            {
                throw new InvalidDataException($"The deletion vector of {input.Path} runs past its payload");
            }
            var count = DeleteOutput.CountPositions(input.Path, decoded, rows);
            if (count != vector.Cardinality)
            {
                throw new InvalidDataException($"The deletion vector of {input.Path} holds {count} rows, its descriptor says {vector.Cardinality}");
            }
            return decoded;
        }

        private async Task PublishAsync(string outputPath, long length, string statistics, CancellationToken cancellationToken)
        {
            var name = $"part-00000-{Guid.NewGuid()}{_bin.Codec.FileExtension}";
            if (!_context.Publish)
            {
                _result.Status = CompactionStatus.Copied;
                _result.Output = Add(name, length, statistics);
                _result.LocalOutput = outputPath;
                return;
            }
            var transport = _context.Transport;
            var destination = _context.Table.Combine(name);
            var native = transport.NativePath(destination);
            // A local move, or the chunked upload's parts
            _result.Requests += native != null ? 1 : 1 + (length + StowageCheckpointTransport.DefaultChunkSize - 1) / StowageCheckpointTransport.DefaultChunkSize;
            if (native != null)
            {
                await transport.ChargeNativeAsync(1, length, cancellationToken);
            }
            try
            {
                if (native != null)
                {
                    PublishLocal(outputPath, length, native);
                }
                else
                {
                    await transport.UploadAsync(outputPath, destination, _memory, cancellationToken);
                    var stored = await transport.SizeAsync(destination, cancellationToken);
                    if (stored != length)
                    {
                        throw new IOException($"The output {name} holds {stored} bytes after upload, {length} written");
                    }
                    _context.Scratch.Delete(outputPath);
                }
            }
            catch
            {
                await DeleteAttemptAsync(destination, native);
                throw;
            }
            _result.Status = CompactionStatus.Copied;
            _result.Output = Add(name, length, statistics);
        }

        // To the data name directly, so the sweep reaps a torn move
        private void PublishLocal(string outputPath, long length, string native)
        {
            var directory = Path.GetDirectoryName(native)!;
            var free = _context.FreeSpaceForTests != null ? _context.FreeSpaceForTests(directory) : CheckpointPublisher.FreeSpaceOf(directory);
            if (free < length + CheckpointPublisher.FreeSpaceMargin)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.DiskCap, DeclineDependency.None, $"The table's volume has {free} bytes free for a {length} byte output");
            }
            _context.FaultForTests?.Invoke("move");
            File.Move(outputPath, native, false);
            _context.FaultForTests?.Invoke("sync");
            // Across file systems the move copied, so the file is made durable
            using (var file = new FileStream(native, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
            {
                file.Flush(true);
            }
            // The scratch copy is gone, so its charge goes
            if (File.Exists(outputPath))
            {
                _context.Scratch.Delete(outputPath);
            }
            else
            {
                _context.Scratch.Ledger.Release(_context.Scratch.Release(outputPath));
            }
        }

        // Deletes what may have landed; failing that, the sweep reaps it
        private async Task DeleteAttemptAsync(IOPath destination, string? native)
        {
            try
            {
                if (native != null)
                {
                    File.Delete(native);
                }
                else
                {
                    await _context.Storage.Rm(destination, CancellationToken.None);
                }
            }
            catch (Exception)
            {
                // Best effort: the publish failure is what the caller sees
            }
        }

        // An output never carries a vector and changes no data (plan §4.3.6)
        private DeltaAddAction Add(string name, long length, string statistics) => new DeltaAddAction()
        {
            Path = name,
            PartitionValues = new Dictionary<string, string>(),
            Size = length,
            ModificationTime = _context.Clock.GetUtcNow().ToUnixTimeMilliseconds(),
            DataChange = false,
            Statistics = statistics
        };

        private static JsonSerializerOptions StatisticsOptions(DeltaStructType table)
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new DeltaStatisticsConverter(table));
            return options;
        }

        private void Decline(int index, CompactionReason reason, bool alone, string detail)
        {
            _result.Declines.Add(new InputDecline(index, reason, alone, detail));
        }

        private void Abandon(CompactionReason? reason, string detail)
        {
            _result.Status = CompactionStatus.Abandoned;
            _result.Reason = reason;
            _result.Detail = detail;
        }

        private void DeleteLocal(LocalFile file)
        {
            if (file.Owned)
            {
                _context.Scratch.Delete(file.Path);
            }
        }

        // An input done: its summary and scratch go
        private void Release(Admitted admitted)
        {
            _memory.Release(admitted.Held);
            admitted.Held = 0;
            DeleteLocal(admitted.File);
        }

        private void Cleanup()
        {
            foreach (var admitted in _admitted.Where(x => x.Held > 0))
            {
                Release(admitted);
            }
            foreach (var file in _vectorFiles.Values)
            {
                DeleteLocal(file);
            }
            _vectorFiles.Clear();
            if (_outputCharge > 0)
            {
                _context.Scratch.Ledger.Release(_outputCharge);
                _outputCharge = 0;
            }
            if (_result.Status == CompactionStatus.Abandoned)
            {
                _result.BytesWasted = _result.BytesWritten;
            }
            _result.NativeOpens = _memory.NativeOpens;
            _result.ManagedPeak = _memory.ManagedPeak;
            _result.NativePeak = _memory.NativePeak;
        }

        private sealed class VectorAllowanceException : Exception
        {
            public VectorAllowanceException(long read, long? length = null)
            {
                Read = read;
                Length = length;
            }

            // Bytes the download read before it stopped
            public long Read { get; }

            // The container's length, when known
            public long? Length { get; }
        }

        // Stops a download that passes the vector allowance
        private sealed class CappedStream : Stream
        {
            private readonly Stream _inner;
            private readonly long _room;
            private long _written;

            public CappedStream(Stream inner, long room)
            {
                _inner = inner;
                _room = room;
            }

            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override long Length => _written;

            public override long Position { get => _written; set => throw new NotSupportedException(); }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Grow(count);
                _inner.Write(buffer, offset, count);
            }

            public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                Grow(buffer.Length);
                await _inner.WriteAsync(buffer, cancellationToken);
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
            }

            private void Grow(int count)
            {
                _written += count;
                if (_written > _room)
                {
                    throw new VectorAllowanceException(_written);
                }
            }

            public override void Flush() => _inner.Flush();

            public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }
                base.Dispose(disposing);
            }

            public override async ValueTask DisposeAsync()
            {
                await _inner.DisposeAsync();
                await base.DisposeAsync();
            }
        }
    }
}
