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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using ParquetSharp;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Reads data files a row group at a time, for A10
    internal static class AdmissionAudit
    {
        public static async Task<int> RunAsync(IReadOnlyDictionary<string, string> options, TextWriter output)
        {
            var batchRows = int.Parse(options.GetValueOrDefault("batch", "65536"), CultureInfo.InvariantCulture);
            var paths = Inputs(options);
            if (paths.Count == 0)
            {
                output.WriteLine("Give --path <directory or file>, or --groups <n> --rows <r> --scratch <dir>");
                return ManualRun.HarnessError;
            }
            // A file built here leaves garbage, collect it before sampling
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var pool = MemoryPool.SystemMemoryPool();
            long groups = 0;
            long rows = 0;
            long maxBatch = 0;
            long poolFormula = 0;
            long fullFormula = 0;
            double steadyRatio = 0;
            long leak = 0;
            long growth = 0;
            long managedGrowth = 0;
            long firstOpenPeak = -1;
            long retainedAtOpen = 0;
            long withBatch = 0;
            string? worst = null;
            var declined = 0;
            var started = Stopwatch.GetTimestamp();
            var privateStart = PrivateBytes();
            var managedStart = GC.GetGCMemoryInfo().TotalCommittedBytes;
            var peak = new PeakWorkingSet();
            await using (peak)
            {
                foreach (var path in paths)
                {
                    (long Pool, long Full) formula;
                    try
                    {
                        formula = Formula(path, batchRows);
                    }
                    catch (DataFileDeclinedException)
                    {
                        declined++;
                        continue;
                    }
                    var (filePool, fileFull) = formula;
                    if (filePool > poolFormula)
                    {
                        poolFormula = filePool;
                        worst = path;
                    }
                    fullFormula = Math.Max(fullFormula, fileFull);
                    long fileWithBatch = 0;
                    var before = pool.BytesAllocated;
                    using var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, batchRows, FooterLength(path)));
                    var open = pool.BytesAllocated;
                    retainedAtOpen = Math.Max(retainedAtOpen, open - before);
                    // The pool's peak is process-wide, so only the first open has its own
                    firstOpenPeak = firstOpenPeak < 0 ? pool.MaxMemory : firstOpenPeak;
                    var metadata = file.LowLevel.FileMetaData;
                    var leaves = Enumerable.Range(0, metadata.NumColumns).ToArray();
                    long firstRss = 0;
                    long firstManaged = 0;
                    for (int g = 0; g < metadata.NumRowGroups; g++)
                    {
                        await foreach (var batch in file.ReadRowGroup(g, leaves))
                        {
                            using (batch)
                            {
                                rows += batch.Length;
                                maxBatch = Math.Max(maxBatch, Bytes(batch));
                                withBatch = Math.Max(withBatch, pool.BytesAllocated - open);
                                fileWithBatch = Math.Max(fileWithBatch, pool.BytesAllocated - before);
                            }
                        }
                        groups++;
                        leak = Math.Max(leak, pool.BytesAllocated - open);
                        firstRss = g == 0 ? Rss() : firstRss;
                        firstManaged = g == 0 ? GC.GetGCMemoryInfo().TotalCommittedBytes : firstManaged;
                    }
                    steadyRatio = Math.Max(steadyRatio, (double)fileWithBatch / filePool);
                    growth = Math.Max(growth, Rss() - firstRss);
                    managedGrowth = Math.Max(managedGrowth, GC.GetGCMemoryInfo().TotalCommittedBytes - firstManaged);
                }
            }
            output.WriteLine(JsonSerializer.Serialize(new
            {
                record = "admission-audit",
                batchRows,
                files = paths.Count,
                groups,
                rows,
                seconds = Stopwatch.GetElapsedTime(started).TotalSeconds,
                poolBackend = pool.BackendName,
                poolMax = pool.MaxMemory,
                poolPeakAtFirstOpen = firstOpenPeak,
                poolRetainedAtOpen = retainedAtOpen,
                poolWithBatch = withBatch,
                maxBatchBytes = maxBatch,
                poolFormula,
                fullFormula,
                poolPeakOverFormula = poolFormula == 0 ? 0 : (double)pool.MaxMemory / poolFormula,
                steadyPoolOverFormula = steadyRatio,
                poolLeftAfterGroups = leak,
                rssPeakAboveStart = peak.Peak,
                rssOverFormula = fullFormula == 0 ? 0 : (double)peak.Peak / fullFormula,
                managedCommittedAboveStart = GC.GetGCMemoryInfo().TotalCommittedBytes - managedStart,
                rssLessManagedOverFormula = fullFormula == 0 ? 0 : (double)(peak.Peak - Math.Max(0, GC.GetGCMemoryInfo().TotalCommittedBytes - managedStart)) / fullFormula,
                rssGrowthAcrossGroups = growth,
                managedGrowthAcrossGroups = managedGrowth,
                privateBytesAboveStart = PrivateBytes() - privateStart,
                declinedFiles = declined,
                worstFile = worst
            }));
            return ManualRun.Passed;
        }

        // Admits files in bins of 50, counting batch sizes and declines
        public static Task<int> GateAsync(IReadOnlyDictionary<string, string> options, TextWriter output)
        {
            var target = long.Parse(options.GetValueOrDefault("target", "32"), CultureInfo.InvariantCulture) << 20;
            var settings = new AdmissionSettings(CompactionBin.FlushBytesFor(target), Compression.Zstd);
            var paths = Inputs(options);
            var batches = new SortedDictionary<long, int>();
            var reasons = new SortedDictionary<string, int>(StringComparer.Ordinal);
            string? firstDecline = null;
            long admittedBytes = 0, totalBytes = 0, largeBatchBytes = 0;
            var prefix = new List<InputSummary>();
            OutputShape? shape = null;
            foreach (var path in paths)
            {
                var length = new FileInfo(path).Length;
                totalBytes += length;
                var memory = new JobMemory();
                var table = TableOf(path);
                shape ??= new OutputShape(table);
                try
                {
                    var (scan, plan) = DataFileAdmission.Preflight(path, table, false, memory);
                    var summary = DataFileAdmission.Summarize(scan, plan, shape);
                    memory.Release(scan.ChargedBytes);
                    var admission = DataFileAdmission.Admit(prefix, summary, shape, settings);
                    if (admission.Reason != null)
                    {
                        firstDecline ??= admission.Detail;
                        reasons[CompactionReasons.Name(admission.Reason.Value)] = reasons.GetValueOrDefault(CompactionReasons.Name(admission.Reason.Value)) + 1;
                        continue;
                    }
                    summary.Candidate = admission.Candidate;
                    var rows = InputSummary.BatchRows(admission.Candidate);
                    batches[rows] = batches.GetValueOrDefault(rows) + 1;
                    admittedBytes += length;
                    largeBatchBytes += rows >= 1024 ? length : 0;
                    prefix.Add(summary);
                    if (prefix.Count == 50)
                    {
                        prefix.Clear();
                    }
                }
                catch (DataFileDeclinedException e)
                {
                    var name = CompactionReasons.Name(e.Reason);
                    reasons[name] = reasons.GetValueOrDefault(name) + 1;
                }
            }
            output.WriteLine(JsonSerializer.Serialize(new
            {
                record = "admission-gate",
                targetBytes = target,
                flushBytes = settings.FlushBytes,
                files = paths.Count,
                admittedFiles = batches.Values.Sum(),
                admittedBytesShare = (double)admittedBytes / totalBytes,
                batchAtLeast1024BytesShare = (double)largeBatchBytes / totalBytes,
                batches,
                reasons,
                firstDecline
            }));
            return Task.FromResult(ManualRun.Passed);
        }

        // A sink file's table is its own schema, flat leaves and structs
        private static Delta.StructType TableOf(string path)
        {
            using var file = new ParquetFileReader(path);
            var fields = new List<Delta.StructField>();
            using var root = file.FileMetaData.Schema.GroupNode;
            foreach (var node in root.Fields)
            {
                using (node)
                {
                    fields.Add(new Delta.StructField(node.Name, DeltaType(node), true, new Dictionary<string, object>()));
                }
            }
            return new Delta.StructType(fields);
        }

        private static FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType DeltaType(ParquetSharp.Schema.Node node)
        {
            if (node is ParquetSharp.Schema.GroupNode group)
            {
                return new Delta.StructType(group.Fields.Select(x => { using (x) { return new Delta.StructField(x.Name, DeltaType(x), true, new Dictionary<string, object>()); } }).ToList());
            }
            var primitive = (ParquetSharp.Schema.PrimitiveNode)node;
            using var logical = primitive.LogicalType;
            return (primitive.PhysicalType, logical) switch
            {
                (PhysicalType.Boolean, _) => new Delta.BooleanType(),
                (PhysicalType.Int32, DateLogicalType) => new Delta.DateType(),
                (PhysicalType.Int32, IntLogicalType { BitWidth: 8 }) => new Delta.ByteType(),
                (PhysicalType.Int32, IntLogicalType { BitWidth: 16 }) => new Delta.ShortType(),
                (PhysicalType.Int32, _) => new Delta.IntegerType(),
                (PhysicalType.Int64, TimestampLogicalType) => new Delta.TimestampType(),
                (PhysicalType.Int64, _) => new Delta.LongType(),
                (PhysicalType.Float, _) => new Delta.FloatType(),
                (PhysicalType.Double, _) => new Delta.DoubleType(),
                (_, DecimalLogicalType d) => new Delta.DecimalType(d.Precision, d.Scale),
                (PhysicalType.ByteArray, StringLogicalType) => new Delta.StringType(),
                _ => new Delta.BinaryType()
            };
        }

        // The pool-visible and whole native formulas, M2's first check of A10
        private static (long Pool, long Full) Formula(string path, int batchRows)
        {
            var memory = new JobMemory();
            var scan = DataPageHeaderScan.Scan(path, memory, schema => Enumerable.Repeat(true, schema.Leaves.Count).ToArray());
            // A footer past the speculative read is copied beside it
            var speculative = Math.Min(NativeConstants.FooterReadSize, new FileInfo(path).Length);
            long pool = scan.Footer.FooterLength + 8 > speculative ? speculative + scan.Footer.FooterLength + 8 : speculative;
            long full = 0;
            for (int g = 0; g < scan.Groups.Length; g++)
            {
                var (decode, groupPool) = scan.Decode(g);
                var (batch, recordReader, _) = scan.ReadBatch(g, batchRows);
                pool = Math.Max(pool, groupPool + batch + recordReader);
                full = Math.Max(full, decode + batch + recordReader);
            }
            memory.Release(scan.ChargedBytes);
            return (pool, scan.Metadata + NativeConstants.ReaderFixed + full);
        }

        private static List<string> Inputs(IReadOnlyDictionary<string, string> options)
        {
            if (options.TryGetValue("groups", out var groups))
            {
                return new List<string>() { ManyGroups(options["scratch"], int.Parse(groups, CultureInfo.InvariantCulture), int.Parse(options.GetValueOrDefault("rows", "2000"), CultureInfo.InvariantCulture)) };
            }
            var path = options.GetValueOrDefault("path", "");
            var files = File.Exists(path) ? new List<string>() { path } : Directory.Exists(path) ? Directory.EnumerateFiles(path, "*.parquet").Order(StringComparer.Ordinal).ToList() : new List<string>();
            // The largest files alone, so the pool's peak is theirs
            return options.TryGetValue("largest", out var largest) ? files.OrderByDescending(x => new FileInfo(x).Length).Take(int.Parse(largest, CultureInfo.InvariantCulture)).ToList() : files;
        }

        // One file of many small groups with the sink's settings
        private static string ManyGroups(string scratch, int groups, int rows)
        {
            var path = Path.Join(scratch, $"many-groups-{groups}x{rows}.parquet");
            var schema = new Schema.Builder()
                .Field(new Field("id", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Field("name", Apache.Arrow.Types.StringType.Default, true))
                .Field(new Field("value", Apache.Arrow.Types.DoubleType.Default, true))
                .Field(new Field("flag", Apache.Arrow.Types.BooleanType.Default, true))
                .Build();
            using var writer = new ParquetSharp.Arrow.FileWriter(path, schema, DeltaParquetCompression.GetWriterProperties(Compression.Zstd));
            var random = new Random(7);
            for (int g = 0; g < groups; g++)
            {
                var ids = new Int64Array.Builder();
                var names = new StringArray.Builder();
                var values = new DoubleArray.Builder();
                var flags = new BooleanArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    ids.Append((long)g * rows + i);
                    names.Append($"n{random.Next(100_000)}-{random.Next(1_000)}");
                    values.Append(random.NextDouble());
                    flags.Append(random.Next(2) == 0);
                }
                using var batch = new RecordBatch(schema, new IArrowArray[] { ids.Build(), names.Build(), values.Build(), flags.Build() }, rows);
                writer.WriteRecordBatch(batch, rows);
            }
            writer.Close();
            return path;
        }

        // The Thrift footer's length, from the file's last eight bytes
        private static long FooterLength(string path)
        {
            using var stream = File.OpenRead(path);
            Span<byte> tail = stackalloc byte[8];
            stream.Seek(-8, SeekOrigin.End);
            stream.ReadExactly(tail);
            return BinaryPrimitives.ReadInt32LittleEndian(tail);
        }

        private static long Bytes(RecordBatch batch)
        {
            return Enumerable.Range(0, batch.ColumnCount).Sum(c => Bytes(batch.Column(c).Data));
        }

        private static long Bytes(ArrayData data)
        {
            return data.Buffers.Sum(x => (long)x.Length) + (data.Children?.Sum(Bytes) ?? 0);
        }

        private static long PrivateBytes()
        {
            using var process = Process.GetCurrentProcess();
            return process.PrivateMemorySize64;
        }

        private static long Rss()
        {
            using var process = Process.GetCurrentProcess();
            return process.WorkingSet64;
        }
    }
}
