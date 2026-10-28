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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using ParquetSharp;
using ParquetSharp.Schema;
using Stowage;
using System.Collections;
using System.Diagnostics;
using System.Text.Json;
using Xunit.Abstractions;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Gate B2c and the R7 decision, report only
    [Collection("Probes")]
    public class DeleteOutputProbes
    {
        private const int Rows = 100_000;
        private static readonly int[] s_densities = new[] { 1, 5, 10, 20, 50 };

        private readonly ITestOutputHelper _output;

        public DeleteOutputProbes(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task B2cDensity()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B2c density, 20 columns, {Rows} rows, deletion vectors on");
            var source = WriteFile(int96: false);
            foreach (var density in s_densities)
            {
                var output = new List<double>();
                var read = new List<double>();
                var cpu = new List<double>();
                var files = 0;
                var vectors = 0;
                for (int run = 0; run <= ProbeKit.Runs; run++)
                {
                    var deleted = Positions(density, run);
                    var result = await Run(source, deleted, deletionVectors: true);
                    if (run > 0)
                    {
                        output.Add(result.OutputMs);
                        cpu.Add(result.CpuMs);
                        read.Add(result.NextReadMs);
                        files = Math.Max(files, result.Files);
                        vectors += result.Files == 0 ? 1 : 0;
                    }
                }
                kit.Write($"{density} % | vector in {vectors} of {ProbeKit.Runs} runs | output {ProbeKit.Spread(output)} ms | cpu {ProbeKit.Spread(cpu)} ms | files added {files} | next read {ProbeKit.Spread(read)} ms");
            }
        }

        [Fact]
        public async Task B2cPrecheckShare()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B2c pre-check share, INT96 source, 20 columns, {Rows} rows, 50 % deleted, deletion vectors off");
            var source = WriteFile(int96: true);
            var shares = new List<double>();
            var precheck = new List<double>();
            var total = new List<double>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                var result = await Run(source, Positions(50, run), deletionVectors: false);
                var watch = Stopwatch.StartNew();
                using (var file = await ParquetSharpReader.OpenDataFile(result.Storage, "/t", "data.parquet", forDeletes: true))
                {
                    var binding = FileBinding.Create(Table(), file, "t");
                    for (int group = 0; group < file.RowGroupStarts.Length; group++)
                    {
                        TimestampPrecision.FlagRowGroup(file.LowLevel, file.Path, group, binding.PrecisionLeaves);
                    }
                }
                watch.Stop();
                if (run > 0)
                {
                    precheck.Add(watch.Elapsed.TotalMilliseconds);
                    total.Add(result.OutputMs);
                    shares.Add(watch.Elapsed.TotalMilliseconds * 100 / result.OutputMs);
                }
            }
            kit.Write($"pre-check {ProbeKit.Spread(precheck)} ms | rewrite {ProbeKit.Spread(total)} ms | share {ProbeKit.Spread(shares)} %");
        }

        [Fact]
        public async Task R7MembershipShare()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            // CopyFrom now builds a mask per batch, so this shows what is left
            var kit = new ProbeKit(_output, $"R7, copy with the claims against a precomputed mask, 20 columns, {Rows} rows, 50 % deleted");
            var source = WriteFile(int96: false);
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            await WriteBytes(storage, "/t/data.parquet", source);
            var claims = new List<double>();
            var masked = new List<double>();
            var claimsWall = new List<double>();
            var maskedWall = new List<double>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                var deleted = Positions(50, run);
                // Alternated, so neither always runs second
                foreach (var mask in run % 2 == 0 ? new[] { false, true } : new[] { true, false })
                {
                    IDeleteVector vector = mask ? new MaskVector(deleted, Rows) : Claims(deleted);
                    using var file = await ParquetSharpReader.OpenDataFile(storage, "/t", "data.parquet", forDeletes: true);
                    var binding = FileBinding.Create(Table(), file, "t");
                    var writer = ParquetSharpWriter.ForRewrite(Table(), binding, null);
                    writer.NewBatch();
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    var cpu = CpuMs();
                    var watch = Stopwatch.StartNew();
                    await writer.CopyFrom(file, binding, vector, _ => false, () => Task.CompletedTask);
                    var wall = watch.Elapsed.TotalMilliseconds;
                    var spent = CpuMs() - cpu;
                    if (run > 0)
                    {
                        (mask ? masked : claims).Add(spent);
                        (mask ? maskedWall : claimsWall).Add(wall);
                    }
                }
            }
            var share = (ProbeKit.Median(claims) - ProbeKit.Median(masked)) * 100 / ProbeKit.Median(claims);
            var wallShare = (ProbeKit.Median(claimsWall) - ProbeKit.Median(maskedWall)) * 100 / ProbeKit.Median(claimsWall);
            kit.Write($"copy cpu with claims {ProbeKit.Spread(claims)} ms | with a mask {ProbeKit.Spread(masked)} ms | membership share {share:0.#} % (gate: build at >= 10 %)");
            kit.Write($"copy wall with claims {ProbeKit.Spread(claimsWall)} ms | with a mask {ProbeKit.Spread(maskedWall)} ms | share {wallShare:0.#} %");
        }

        private sealed record Result(double OutputMs, double CpuMs, int Files, double NextReadMs, IFileStorage Storage);

        // The delete output on one file, then a read of what is left
        private static async Task<Result> Run(byte[] source, long[] deleted, bool deletionVectors)
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            await WriteBytes(storage, "/t/data.parquet", source);
            var table = Table();
            var options = new JsonSerializerOptions();
            options.Converters.Add(new Internal.Delta.Stats.DeltaStatisticsConverter(table));
            var overlay = new CatalogOverlay(1, PruningLayout.Create(table, Array.Empty<string>(), 8), options, null);
            var actions = new List<DeltaAction>();
            var created = new List<string>();
            var written = new List<string>();
            // The default file size
            const long maxBytes = 100 * 1024 * 1024;
            var output = new DeleteOutput(storage, "/t", "t", table, false, deletionVectors, null, w => w.WrittenBytes >= maxBytes, async writer =>
            {
                var name = $"part-{written.Count}.parquet";
                written.Add(name);
                await writer.WriteData(storage, "/t", name);
                writer.NewBatch();
            });
            var vector = Claims(deleted);
            var record = new DeltaFileRecord() { Path = "data.parquet", Size = source.Length, PartitionValues = new Dictionary<string, string>() };

            var cpu = CpuMs();
            var watch = Stopwatch.StartNew();
            await output.WriteAsync(new Dictionary<int, ModifiableDeleteVector>() { [0] = vector }, new Dictionary<int, DeltaFileRecord>() { [0] = record }, new Dictionary<int, long>() { [0] = Rows }, actions, 1000, overlay, created);
            var outputMs = watch.Elapsed.TotalMilliseconds;
            var cpuMs = CpuMs() - cpu;

            // A vector is read with the source, a rewrite reads its new files
            watch.Restart();
            long live = 0;
            foreach (var path in written.Count == 0 ? new List<string>() { "data.parquet" } : written)
            {
                using var file = await ParquetSharpReader.OpenDataFile(storage, "/t", path, forDeletes: false);
                long position = 0;
                await foreach (var batch in file.ReadBatches(null))
                {
                    using (batch)
                    {
                        for (int i = 0; i < batch.Length; i++)
                        {
                            if (written.Count > 0 || !vector.Contains(position + i))
                            {
                                live++;
                            }
                        }
                        position += batch.Length;
                    }
                }
            }
            var readMs = watch.Elapsed.TotalMilliseconds;
            if (live != Rows - deleted.Length)
            {
                throw new InvalidOperationException($"{live} rows are left, expected {Rows - deleted.Length}");
            }
            return new Result(outputMs, cpuMs, written.Count, readMs, storage);
        }

        private static ModifiableDeleteVector Claims(long[] deleted)
        {
            var vector = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
            foreach (var position in deleted)
            {
                vector.Add(position);
            }
            return vector;
        }

        // A seeded sample of the rows, ascending
        private static long[] Positions(int density, int run)
        {
            var random = new Random(density * 100 + run);
            return Enumerable.Range(0, Rows).Where(_ => random.Next(100) < density).Select(x => (long)x).ToArray();
        }

        private static Delta.StructType Table()
        {
            var fields = new List<Delta.StructField>();
            for (int i = 0; i < 20; i++)
            {
                Internal.Delta.Schema.SchemaBaseType type = i < 10 ? new Delta.LongType() : i < 15 ? new Delta.IntegerType() : i < 19 ? new Delta.StringType() : new Delta.TimestampType();
                fields.Add(new Delta.StructField($"c{i}", type, true, new Dictionary<string, object>()));
            }
            return new Delta.StructType(fields);
        }

        // Ten longs, five ints, four strings and a timestamp, a tenth of each null
        private static byte[] WriteFile(bool int96)
        {
            var random = new Random(7);
            var nodes = new List<Node>();
            for (int i = 0; i < 20; i++)
            {
                nodes.Add(i < 10 ? new PrimitiveNode($"c{i}", Repetition.Optional, LogicalType.None(), PhysicalType.Int64)
                    : i < 15 ? new PrimitiveNode($"c{i}", Repetition.Optional, LogicalType.None(), PhysicalType.Int32)
                    : i < 19 ? new PrimitiveNode($"c{i}", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray)
                    : int96 ? new PrimitiveNode($"c{i}", Repetition.Optional, LogicalType.None(), PhysicalType.Int96)
                    : new PrimitiveNode($"c{i}", Repetition.Optional, LogicalType.Timestamp(true, TimeUnit.Micros), PhysicalType.Int64));
            }
            using var schema = new GroupNode("schema", Repetition.Required, nodes.ToArray());
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x.Compression(Compression.Snappy));
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                using (var rowGroup = writer.AppendRowGroup())
                {
                    for (int c = 0; c < 20; c++)
                    {
                        var definitions = Enumerable.Range(0, Rows).Select(_ => random.Next(10) == 0 ? (short)0 : (short)1).ToArray();
                        var present = definitions.Count(x => x == 1);
                        using var column = rowGroup.NextColumn();
                        switch (column)
                        {
                            case ColumnWriter<long> longs:
                                longs.WriteBatch(Rows, definitions, null, Enumerable.Range(0, present).Select(_ => c == 19 ? random.NextInt64(1_700_000_000_000_000) : random.NextInt64()).ToArray());
                                break;
                            case ColumnWriter<int> ints:
                                ints.WriteBatch(Rows, definitions, null, Enumerable.Range(0, present).Select(_ => random.Next()).ToArray());
                                break;
                            case ColumnWriter<ByteArray> strings:
                                WriteStrings(strings, definitions, present, random);
                                break;
                            case ColumnWriter<Int96> int96s:
                                // Whole seconds of the day, so nothing is flagged
                                int96s.WriteBatch(Rows, definitions, null, Enumerable.Range(0, present).Select(_ =>
                                {
                                    var nanos = random.Next(86_400) * 1_000_000_000L;
                                    return new Int96((int)nanos, (int)(nanos >> 32), 2_460_000 + random.Next(1_000));
                                }).ToArray());
                                break;
                        }
                    }
                }
                writer.Close();
            }
            foreach (var node in nodes)
            {
                node.Dispose();
            }
            return stream.ToArray();
        }

        private static void WriteStrings(ColumnWriter<ByteArray> writer, short[] definitions, int present, Random random)
        {
            var values = Enumerable.Range(0, present).Select(_ => System.Text.Encoding.UTF8.GetBytes($"value-{random.Next(100_000)}")).ToArray();
            var handles = values.Select(x => System.Runtime.InteropServices.GCHandle.Alloc(x, System.Runtime.InteropServices.GCHandleType.Pinned)).ToArray();
            try
            {
                var arrays = values.Select((x, i) => new ByteArray(handles[i].AddrOfPinnedObject(), x.Length)).ToArray();
                writer.WriteBatch(definitions.Length, definitions, null, arrays);
            }
            finally
            {
                foreach (var handle in handles)
                {
                    handle.Free();
                }
            }
        }

        private static async Task WriteBytes(IFileStorage storage, string path, byte[] bytes)
        {
            using var write = await storage.OpenWrite(path);
            await write.WriteAsync(bytes);
        }

        private static double CpuMs()
        {
            using var process = Process.GetCurrentProcess();
            return process.TotalProcessorTime.TotalMilliseconds;
        }

        // The deleted rows as an array, as the batch mask holds them
        private sealed class MaskVector : IDeleteVector
        {
            private readonly bool[] _deleted;
            private readonly long[] _positions;

            public MaskVector(long[] positions, int rows)
            {
                _positions = positions;
                _deleted = new bool[rows];
                foreach (var position in positions)
                {
                    _deleted[position] = true;
                }
            }

            public long Cardinality => _positions.Length;

            public bool Contains(long index) => _deleted[index];

            public IEnumerator<long> GetEnumerator() => ((IEnumerable<long>)_positions).GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}
