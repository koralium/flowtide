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
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using ParquetSharp;
using Stowage;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // P-admit: copies bins, then checks memory, copy time and re-admission
    internal static class CompactionAudit
    {
        private static readonly JsonSerializerOptions s_json = new JsonSerializerOptions() { Converters = { new JsonStringEnumConverter() } };

        // Generated inputs; the table may add fields, compressible shapes fix rows
        private sealed record Shape(Schema File, Schema? Table = null, int Rows = 0, int GroupRows = 0, bool Skew = false, Func<Random, int, int>? Text = null, Func<Random, int, int>? List = null, int Pool = 0);

        private static readonly Dictionary<string, Shape> s_shapes = new Dictionary<string, Shape>(StringComparer.Ordinal)
        {
            ["narrow"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("value", DoubleType.Default))),
            ["strings"] = new Shape(Fields(Enumerable.Range(0, 6).Select(i => Leaf($"s{i}", StringType.Default)).ToArray()), Text: (r, _) => 10 + r.Next(190)),
            ["tailed"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("s", StringType.Default)), Text: (r, _) => (int)Math.Min(1 << 20, 8 / Math.Pow(1 - r.NextDouble(), 1 / 1.2))),
            ["booleans"] = new Shape(Fields(Enumerable.Range(0, 64).Select(i => Leaf($"b{i}", BooleanType.Default)).ToArray())),
            ["nested"] = new Shape(Fields(
                Leaf("s", Struct(Leaf("a", Struct(Leaf("b", Struct(Leaf("c", Int64Type.Default), Leaf("d", StringType.Default))), Leaf("e", BooleanType.Default))), Leaf("f", DoubleType.Default))),
                Leaf("l", new ListType(Leaf("element", Struct(Leaf("x", Int32Type.Default), Leaf("y", StringType.Default))))),
                Leaf("m", new MapType(StringType.Default, Int64Type.Default)))),
            ["dictionary"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("s", StringType.Default)), Rows: 5_000, Text: (_, _) => 4_096, Pool: 16),
            ["giant"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("s", StringType.Default)), Rows: 20_000, Text: (_, row) => row == 0 ? 8 << 20 : 20),
            ["tiny-groups"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("value", DoubleType.Default)), Rows: 10_000, GroupRows: 10),
            ["skew"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("s", StringType.Default)), Skew: true),
            ["missing"] = new Shape(
                Fields(Leaf("id", Int64Type.Default), Leaf("l", new ListType(Leaf("element", Struct(Leaf("x", Int32Type.Default)))))),
                Fields(Leaf("id", Int64Type.Default), Leaf("l", new ListType(Leaf("element", Struct(Wide(Leaf("x", Int32Type.Default)))))), Leaf("w", Struct(Wide()))),
                GroupRows: 2_000, List: (r, _) => r.Next(33)),
            ["decimal"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("d38", new Decimal128Type(38, 4)), Leaf("d9", new Decimal128Type(9, 2)))),
            ["long-list"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("l", new ListType(Int64Type.Default))), Rows: 1, List: (_, _) => 200_000),
            ["byte-lists"] = new Shape(Fields(Leaf("l", new ListType(Int8Type.Default))), Rows: 20_000, List: (r, _) => r.Next(51)),
            ["string-groups"] = new Shape(Fields(Enumerable.Range(0, 6).Select(i => Leaf($"s{i}", StringType.Default)).ToArray()), Rows: 13_000, Text: (r, _) => 10 + r.Next(190)),
            ["small-byte-lists"] = new Shape(Fields(Leaf("l", new ListType(Int8Type.Default))), Rows: 2_000, List: (r, _) => r.Next(51)),
            ["long-lists"] = new Shape(Fields(Leaf("id", Int64Type.Default), Leaf("l", new ListType(Int64Type.Default))), List: (r, _) => r.Next(51)),
            ["bool-lists-5"] = new Shape(Fields(Leaf("l", new ListType(BooleanType.Default))), List: (_, _) => 5),
            ["bool-lists-25"] = new Shape(Fields(Leaf("l", new ListType(BooleanType.Default))), List: (_, _) => 25)
        };

        public static async Task<int> RunAsync(IReadOnlyDictionary<string, string> options, TextWriter output)
        {
            var target = long.Parse(options.GetValueOrDefault("target", "32"), CultureInfo.InvariantCulture) << 20;
            var codec = options.GetValueOrDefault("codec", "zstd") == "snappy" ? new DeltaParquetCodec(Compression.Snappy, ".snappy.parquet") : new DeltaParquetCodec(Compression.Zstd, ".zstd.parquet");
            var scratch = options["scratch"];
            var skip = int.Parse(options.GetValueOrDefault("skip", "0"), CultureInfo.InvariantCulture);
            var count = int.Parse(options.GetValueOrDefault("bins", "1"), CultureInfo.InvariantCulture);
            // Full collections slow the copier, so timing runs turn them off
            var live = options.GetValueOrDefault("live", "on") == "on";
            // Outputs kept for a closer look
            var keep = options.GetValueOrDefault("keep");
            if (keep != null)
            {
                Directory.CreateDirectory(keep);
            }
            // Inputs written in their own process, so the measured one starts clean
            if (options.GetValueOrDefault("generate", "") == "only")
            {
                Generate(s_shapes[options["shape"]], Path.Join(scratch, "inputs"), target, codec.Compression);
                return ManualRun.Passed;
            }
            // A tiny bin first, so the JIT's memory isn't counted
            var warm = Generate(s_shapes["narrow"], Path.Join(scratch, "warm"), 1L << 20, codec.Compression);
            await Copy(warm, warm.Bins[0], 1L << 20, codec, scratch, "warm", 1, live, null);
            var inputs = options.TryGetValue("table", out var dir)
                ? await TableBins(dir, target, options.GetValueOrDefault("dvs", "on") == "on")
                : Generate(s_shapes[options["shape"]], Path.Join(scratch, "inputs"), target, codec.Compression) with { Source = options["shape"] };
            var failed = false;
            foreach (var (bin, index) in inputs.Bins.Select((x, i) => (x, i)).Skip(skip).Take(count))
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var record = await Copy(inputs, bin, target, codec, scratch, $"{inputs.Source}#{index}", 1, live, keep);
                // The planner learns expansion from an abandon, so retry with it
                if (record.Status == CompactionStatus.Abandoned && record.Expansion > 1)
                {
                    output.WriteLine(JsonSerializer.Serialize(record, s_json));
                    record = await Copy(inputs, bin, target, codec, scratch, $"{inputs.Source}#{index}", record.Expansion, live, keep);
                }
                // Bins declined in pass 1 reserve nothing, so skip their ratios
                failed |= record.NativeReserved > 0 && (record.RawRatio > 0.9 || record.NativeRatio > 0.9) || record.LiveOverLedger > 0 || record.Status == CompactionStatus.Copied && record.ReadmitBatch < Math.Min(1_024, record.InputBatch);
                output.WriteLine(JsonSerializer.Serialize(record, s_json));
            }
            return failed ? ManualRun.BarFailed : ManualRun.Passed;
        }

        private sealed record Record(string Bin, long TargetBytes, string Codec, int Inputs, long InputBytes, long InputRows, CompactionStatus Status, string? Reason, string Detail, Dictionary<string, string> Declines, int Copied,
            double Seconds, long OutputBytes, double Expansion, long NativeReserved, long ManagedReserved, long RssPeak, long NativeMeasured, long ManagedMeasured, long ManagedHeld, long ManagedLive, long LiveOverLedger, double NativeRatio, double RawRatio, long InputBatch, long ReadmitBatch, string? ReadmitReason);

        private static async Task<Record> Copy(Inputs source, IReadOnlyList<CompactionInput> inputs, long target, DeltaParquetCodec codec, string scratch, string name, double expansion, bool live, string? keep)
        {
            var directory = new ScratchDirectory(Path.Join(scratch, "s-" + ScratchOwners.ShortId()));
            var memory = new JobMemory();
            try
            {
                using var storage = Files.Of.LocalDisk(source.Root);
                using var job = directory.CreateJob();
                // Live managed bytes at each input and at close, against the ledger then
                var baseline = GC.GetTotalMemory(true);
                long held = 0, over = long.MinValue;
                Action<string>? measure = !live ? null : point =>
                {
                    var bytes = GC.GetTotalMemory(true) - baseline;
                    held = Math.Max(held, bytes);
                    over = Math.Max(over, bytes - memory.Managed);
                };
                var context = new CompactionContext() { Storage = storage, Table = source.Folder, Scratch = job, Memory = memory, Publish = false, FaultForTests = measure };
                var bin = new CompactionBin() { Table = source.Table, TableName = "audit", Codec = codec, TargetBytes = target, Expansion = expansion, Inputs = inputs };
                CompactionResult result;
                var started = Stopwatch.GetTimestamp();
                var sampler = new Sampler();
                await using (sampler)
                {
                    result = await CompactionCopy.RunAsync(context, bin, default);
                }
                var seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
                var (batch, reason) = result.LocalOutput != null ? Readmit(result.LocalOutput, source.Table, target, codec.Compression) : (0, null);
                if (keep != null && result.LocalOutput != null)
                {
                    File.Copy(result.LocalOutput, Path.Join(keep, $"{name.Replace('#', '-')}-{target >> 20}{codec.FileExtension}"), true);
                }
                // The smallest batch a copied input gets alone
                var inputBatch = result.Copied.Select(x => Readmit(Path.Join(source.Root, source.Folder, x.Path), source.Table, target, codec.Compression).Batch).DefaultIfEmpty().Min();
                var declines = result.Declines.GroupBy(x => CompactionReasons.Name(x.Reason)).ToDictionary(x => x.Key, x => $"{x.Count()}: {x.First().Detail}");
                return new Record(name, target, codec.FileExtension, inputs.Count, inputs.Sum(x => x.Size), inputs.Sum(x => x.Rows), result.Status, result.Reason == null ? null : CompactionReasons.Name(result.Reason.Value), result.Detail, declines, result.Copied.Count,
                    seconds, result.BytesWritten, result.Expansion, result.NativePeak, result.ManagedPeak, sampler.Rss, sampler.Native, sampler.Managed, sampler.Held, held, over == long.MinValue ? 0 : over,
                    result.NativePeak == 0 ? 0 : (double)sampler.Native / result.NativePeak, (double)sampler.Rss / Math.Max(1, result.NativePeak + result.ManagedPeak), inputBatch, batch, reason);
            }
            finally
            {
                directory.ReleaseLockForTests();
                Directory.Delete(directory.Root, true);
            }
        }

        // The output read back as a later bin's input, at its own target
        private static (long Batch, string? Reason) Readmit(string path, Delta.StructType table, long target, Compression compression)
        {
            var memory = new JobMemory();
            try
            {
                var (scan, plan) = DataFileAdmission.Preflight(path, table, false, memory);
                var output = new OutputShape(table);
                var summary = DataFileAdmission.Summarize(scan, plan, output);
                memory.Release(scan.ChargedBytes);
                var admission = DataFileAdmission.Admit(System.Array.Empty<InputSummary>(), summary, output, new AdmissionSettings(CompactionBin.FlushBytesFor(target), compression));
                return admission.Reason == null ? (InputSummary.BatchRows(admission.Candidate), null) : (0, $"{CompactionReasons.Name(admission.Reason.Value)}: {admission.Detail}");
            }
            catch (DataFileDeclinedException e)
            {
                return (0, $"{CompactionReasons.Name(e.Reason)}: {e.Message}");
            }
        }

        private sealed record Inputs(string Root, string Folder, Delta.StructType Table, List<List<CompactionInput>> Bins, string Source);

        // Live small files in log order, binned as the planner would
        private static async Task<Inputs> TableBins(string dir, long target, bool vectors)
        {
            using var storage = Files.Of.LocalDisk(dir);
            var head = (await DeltaTransactionReader.ReadTable(storage, CompactionFixture.Table))!;
            var bins = new List<List<CompactionInput>>() { new List<CompactionInput>() };
            double projected = 0;
            long rows = 0;
            foreach (var add in head.AddFiles.Where(x => x.Size < target / 2))
            {
                using var statistics = JsonDocument.Parse(add.Statistics!);
                var records = statistics.RootElement.GetProperty("numRecords").GetInt64();
                var live = records - (vectors ? add.DeletionVector?.Cardinality ?? 0 : 0);
                if (bins[^1].Count > 0 && (bins[^1].Count == CompactionLimits.Default.MaxInputs || projected + (double)add.Size * live / records > target || rows + live > CompactionLimits.Default.MaxRows))
                {
                    bins.Add(new List<CompactionInput>());
                    projected = 0;
                    rows = 0;
                }
                bins[^1].Add(new CompactionInput(add.Path, add.Size, records, vectors ? add.DeletionVector : null));
                projected += (double)add.Size * live / records;
                rows += live;
            }
            return new Inputs(dir, CompactionFixture.Table, head.Schema, bins, Path.GetFileName(Path.TrimEndingDirectorySeparator(dir)) + (vectors ? "" : "-nodv"));
        }

        // Files of about T/16, enough for one bin past T
        private static Inputs Generate(Shape shape, string root, long target, Compression compression)
        {
            var folder = Path.Join(root, "t");
            var manifest = Path.Join(root, "inputs.json");
            var table = Table(shape.Table ?? shape.File);
            if (File.Exists(manifest))
            {
                return new Inputs(root, "t", table, new List<List<CompactionInput>>() { JsonSerializer.Deserialize<List<CompactionInput>>(File.ReadAllText(manifest))! }, "");
            }
            Directory.CreateDirectory(folder);
            var inputs = new List<CompactionInput>();
            var random = new Random(17);
            var pool = shape.Pool == 0 ? null : Enumerable.Range(0, shape.Pool).Select(i => Text(random, shape.Text!(random, i))).ToArray();
            var rows = shape.Rows > 0 ? shape.Rows : 2_000;
            for (int f = 0; f < CompactionLimits.Default.MaxInputs && (f < 2 || inputs.Sum(x => x.Size) < target + target / 4); f++)
            {
                var path = Path.Join(folder, $"in-{f}.parquet");
                using (var writer = new ParquetSharp.Arrow.FileWriter(path, shape.File, DeltaParquetCompression.GetWriterProperties(compression)))
                {
                    var groups = shape.Skew && rows > 100 ? new[] { 100, rows - 100 } : new[] { rows };
                    foreach (var length in groups)
                    {
                        var columns = shape.File.FieldsList.Select(x => Column(x.DataType, x.IsNullable, length, random, shape, pool)).ToArray();
                        using var batch = new RecordBatch(shape.File, columns, length);
                        writer.WriteRecordBatch(batch, shape.GroupRows > 0 ? shape.GroupRows : length);
                    }
                    writer.Close();
                }
                var size = new FileInfo(path).Length;
                inputs.Add(new CompactionInput($"in-{f}.parquet", size, rows, null));
                // The first file sets the rows that make T/16
                rows = f == 0 && shape.Rows == 0 ? (int)Math.Clamp(rows * (target / 16.0) / size, 1, 4_000_000) : rows;
            }
            File.WriteAllText(manifest, JsonSerializer.Serialize(inputs));
            return new Inputs(root, "t", table, new List<List<CompactionInput>>() { inputs }, "");
        }

        private static IArrowArray Column(IArrowType type, bool nullable, int rows, Random random, Shape shape, string[]? pool)
        {
            var valid = Enumerable.Range(0, rows).Select(_ => !nullable || random.Next(10) != 0).ToArray();
            var nulls = valid.Count(x => !x);
            void Each(Action append, Action appendNull)
            {
                for (int i = 0; i < rows; i++)
                {
                    (valid[i] ? append : appendNull)();
                }
            }
            switch (type)
            {
                case Int64Type:
                    var longs = new Int64Array.Builder();
                    Each(() => longs.Append(random.NextInt64()), () => longs.AppendNull());
                    return longs.Build();
                case Int32Type:
                    var ints = new Int32Array.Builder();
                    Each(() => ints.Append(random.Next()), () => ints.AppendNull());
                    return ints.Build();
                case Int8Type:
                    var bytes = new Int8Array.Builder();
                    Each(() => bytes.Append((sbyte)random.Next(4)), () => bytes.AppendNull());
                    return bytes.Build();
                case DoubleType:
                    var doubles = new DoubleArray.Builder();
                    Each(() => doubles.Append(random.NextDouble()), () => doubles.AppendNull());
                    return doubles.Build();
                case BooleanType:
                    var booleans = new BooleanArray.Builder();
                    Each(() => booleans.Append(random.Next(2) == 0), () => booleans.AppendNull());
                    return booleans.Build();
                case StringType:
                    var strings = new StringArray.Builder();
                    var row = 0;
                    Each(() => strings.Append(pool?[random.Next(pool.Length)] ?? Text(random, shape.Text?.Invoke(random, row++) ?? 8 + random.Next(24))), () => strings.AppendNull());
                    return strings.Build();
                case Decimal128Type d:
                    var decimals = new Decimal128Array.Builder(d);
                    Each(() => decimals.Append(Math.Round((decimal)random.NextDouble() * 10_000m, d.Scale)), () => decimals.AppendNull());
                    return decimals.Build();
                case StructType s:
                    return new StructArray(s, rows, s.Fields.Select(x => Column(x.DataType, x.IsNullable, rows, random, shape, pool)).ToArray(), Bitmap(valid), nulls);
                case ListType or MapType:
                    var lengths = Enumerable.Range(0, rows).Select(i => valid[i] ? shape.List?.Invoke(random, i) ?? random.Next(9) : 0).ToArray();
                    var offsets = new ArrowBuffer.Builder<int>().Append(0);
                    var total = 0;
                    foreach (var length in lengths)
                    {
                        offsets.Append(total += length);
                    }
                    if (type is MapType map)
                    {
                        return new MapArray(map, rows, offsets.Build(), Column(map.KeyValueType, false, total, random, shape, pool), Bitmap(valid), nulls);
                    }
                    var list = (ListType)type;
                    return new ListArray(list, rows, offsets.Build(), Column(list.ValueDataType, list.ValueField.IsNullable, total, random, shape, pool), Bitmap(valid), nulls);
                default:
                    throw new NotSupportedException(type.Name);
            }
        }

        private static ArrowBuffer Bitmap(bool[] valid)
        {
            var bitmap = new ArrowBuffer.BitmapBuilder(valid.Length);
            foreach (var x in valid)
            {
                bitmap.Append(x);
            }
            return bitmap.Build();
        }

        private static string Text(Random random, int length) => string.Create(length, random, (span, r) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = (char)('a' + r.Next(26));
            }
        });

        private static Field Leaf(string name, IArrowType type) => new Field(name, type, true);

        private static StructType Struct(params Field[] fields) => new StructType(fields);

        // Forty string fields the files never have
        private static Field[] Wide(params Field[] first) => first.Concat(Enumerable.Range(0, 40).Select(i => Leaf($"f{i}", StringType.Default))).ToArray();

        private static Schema Fields(params Field[] fields) => new Schema(fields, null);

        private static Delta.StructType Table(Schema schema) => new Delta.StructType(schema.FieldsList.Select(DeltaField).ToList());

        private static Delta.StructField DeltaField(Field field) => new Delta.StructField(field.Name, DeltaType(field.DataType), field.IsNullable, new Dictionary<string, object>());

        private static Internal.Delta.Schema.SchemaBaseType DeltaType(IArrowType type) => type switch
        {
            Int64Type => new Delta.LongType(),
            Int32Type => new Delta.IntegerType(),
            Int8Type => new Delta.ByteType(),
            DoubleType => new Delta.DoubleType(),
            BooleanType => new Delta.BooleanType(),
            StringType => new Delta.StringType(),
            Decimal128Type d => new Delta.DecimalType(d.Precision, d.Scale),
            StructType s => new Delta.StructType(s.Fields.Select(DeltaField).ToList()),
            MapType m => new Delta.MapType(DeltaType(m.KeyField.DataType), DeltaType(m.ValueField.DataType), true),
            ListType l => new Delta.ArrayType() { ElementType = DeltaType(l.ValueDataType), ContainsNull = true },
            _ => throw new NotSupportedException(type.Name)
        };

        // RSS above the start, less the GC's growth, sampled every millisecond
        private sealed class Sampler : IAsyncDisposable
        {
            private readonly long _rss = Environment.WorkingSet;
            private readonly long _managed = ManagedBytes(GC.GetGCMemoryInfo());
            private readonly long _held = HeapHeld(GC.GetGCMemoryInfo());
            private readonly Thread _thread;
            private volatile bool _stop;

            public Sampler()
            {
                _thread = new Thread(() =>
                {
                    while (!_stop)
                    {
                        Sample();
                        Thread.Sleep(1);
                    }
                });
                _thread.Start();
            }

            public long Rss { get; private set; }

            public long Native { get; private set; }

            public long Managed { get; private set; }

            // The heap after the last GC, less its free space
            public long Held { get; private set; }

            // Committed as of the last GC, or allocated since, whichever is more
            private static long ManagedBytes(GCMemoryInfo info) => Math.Max(info.TotalCommittedBytes, GC.GetTotalMemory(false));

            private static long HeapHeld(GCMemoryInfo info) => info.HeapSizeBytes - info.FragmentedBytes;

            private void Sample()
            {
                var info = GC.GetGCMemoryInfo();
                var rss = Environment.WorkingSet - _rss;
                var managed = Math.Max(0, ManagedBytes(info) - _managed);
                Held = Math.Max(Held, HeapHeld(info) - _held);
                Rss = Math.Max(Rss, rss);
                Managed = Math.Max(Managed, managed);
                Native = Math.Max(Native, rss - managed);
            }

            public ValueTask DisposeAsync()
            {
                _stop = true;
                _thread.Join();
                Sample();
                return ValueTask.CompletedTask;
            }
        }
    }
}
