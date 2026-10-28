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
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection("Probes")]
    public class MatcherProbes
    {
        private const string OrderInsert = @"
            CREATE TABLE test (
                orderkey INT,
                orderdate TIMESTAMP
            );

            INSERT INTO test
            SELECT orderkey, orderdate FROM orders
        ";

        private readonly ITestOutputHelper _output;

        public MatcherProbes(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task B2bPrecisionWorstCase()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            const int rows = 10_000_000;
            const int hit = 500;
            var kit = new ProbeKit(_output, $"B2b precision worst case, one INT96 row group of {rows} rows, the only hit at row {hit}");
            var samples = new List<ProbeSample>();
            var matching = new List<DeleteStatistics>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                const string name = "b2b_precision";
                await using var table = new ProbeTable(name, ProbeTable.NewRoot(), options => options.EnableColumnMappingOnNewTables = false, keepFiles: false);
                table.Stream.GenerateUsers(1);
                table.Stream.GenerateOrders(1);
                await table.Stream.StartStream(OrderInsert.Replace("test", name));
                await WaitForVersion(table.Storage, name, table.Stream, 0, TimeSpan.FromMinutes(5));
                var order = table.Stream.Orders[0];
                await ForeignRewrite.Commit(table.Storage, name, await WorstCaseFile(table, name, rows, hit));
                await table.Settle();

                var sample = await table.Measure(() => table.Stream.DeleteOrder(order), TimeSpan.FromMinutes(2));
                if (sample == null)
                {
                    kit.Write("D=1 | did not finish within 2 minutes");
                    return;
                }
                if (run > 0)
                {
                    samples.Add(sample.Value);
                    matching.Add(table.Stream.Sinks[0].DeleteStatisticsForTests!);
                }
            }
            kit.Report("D=1 delete", samples);
            DeleteProbes.ReportMatching(kit, "D=1", matching);
        }

        [Fact]
        public void B2bClaimMemory()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            const int claims = 1_000_000;
            var kit = new ProbeKit(_output, $"B2b claim memory, {claims} claimed positions");
            var live = new List<double>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                var before = GC.GetTotalMemory(true);
                var vector = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
                for (long i = 0; i < claims; i++)
                {
                    vector.Add(i * 3);
                }
                var after = GC.GetTotalMemory(true);
                GC.KeepAlive(vector);
                if (run > 0)
                {
                    live.Add((after - before) / 1048576.0);
                }
            }
            kit.Write($"D={claims} | live claim memory {ProbeKit.Spread(live)} MiB | gate 64 MiB");
        }

        [Theory]
        [InlineData(1, false, 8, 100_000)]
        [InlineData(1, true, 8, 100_000)]
        [InlineData(8, false, 8, 100_000)]
        [InlineData(8, true, 8, 100_000)]
        [InlineData(1, false, 8, 1_000_000)]
        [InlineData(8, true, 8, 1_000_000)]
        [InlineData(1, false, 2_000, 100_000)]
        [InlineData(8, true, 2_000, 100_000)]
        [InlineData(1, false, 8, 100_000, 64)]
        [InlineData(1, false, 8, 100_000, 0, 64)]
        [InlineData(1, false, 8, 100_000, 0, 64, true)]
        public void B2bBatchPeak(int pruning, bool changeData, int nameLength, int rowCap, int unwritten = 0, int structFields = 0, bool partial = false)
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var structs = structFields == 0 ? "" : $", a struct of {structFields} longs, {(partial ? "one field set" : "null")}";
            var kit = new ProbeKit(_output, $"B2b one delete batch's memory, P = {pruning}, CDF {(changeData ? "on" : "off")}, names of {nameLength} B, {unwritten} unwritten long columns{structs}, up to {rowCap} rows or 128 MiB by R3's estimate");
            var fields = Enumerable.Range(0, pruning == 1 ? 1 : 7).Select(i => new Delta.StructField($"k{i}", new Delta.LongType(), true, new Dictionary<string, object>()))
                .Append(new Delta.StructField("name", new Delta.StringType(), true, new Dictionary<string, object>())).ToList();
            if (structFields > 0)
            {
                var inner = Enumerable.Range(0, structFields).Select(i => new Delta.StructField($"v{i}", new Delta.LongType(), true, new Dictionary<string, object>())).ToList();
                fields.Add(new Delta.StructField("s", new Delta.StructType(inner), true, new Dictionary<string, object>()));
            }
            var names = fields.Select(x => x.Name).ToList();
            fields.AddRange(Enumerable.Range(0, unwritten).Select(i => new Delta.StructField($"u{i}", new Delta.LongType(), true, new Dictionary<string, object>())));
            var table = new Delta.StructType(fields);
            var layout = PruningLayout.Create(table, names, pruning);
            var comparers = table.Fields.Select(x => x.Type.Accept(new ArrowComparerVisitor())).ToArray();
            var values = Enumerable.Range(0, 1_000).Select(i => i.ToString("D4") + new string('x', Math.Max(0, nameLength - 4))).ToArray();
            var columns = names.Select(_ => FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance)).ToArray();
            var name = names.IndexOf("name");
            for (int i = 0; i < rowCap; i++)
            {
                for (int c = 0; c < name; c++)
                {
                    columns[c].Add(new Int64Value(i * 7L + c));
                }
                columns[name].Add(new StringValue(values[i % values.Length]));
                if (structFields > 0)
                {
                    if (partial)
                    {
                        columns[^1].Add(new StructValue(StructHeader.Create("v0"), new Int64Value(i)));
                    }
                    else
                    {
                        columns[^1].Add(NullValue.Instance);
                    }
                }
            }
            using var data = new EventBatchData(columns);
            var limits = DeleteLimits.Default;
            var samples = new List<(int Rows, double Written, double Estimate, double Copy, double Matching)>();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                var managedBefore = GC.GetTotalMemory(true);
                var nativeBefore = NativeAllocated();
                // As the sink fills a batch, then resolves it
                var writer = new ParquetSharpWriter(table, names, countNulls: true);
                writer.NewBatch();
                // As the sink counts a row
                var rowBytes = 32 + 4 * layout.Columns.Count + (changeData ? 8 : 0) + writer.NullBytesPerRow * limits.WrittenSlack;
                var probes = new ProbeBatch(layout);
                var demand = new List<long>();
                for (int i = 0; i < rowCap; i++)
                {
                    var row = new ColumnRowReference() { referenceBatch = data, RowIndex = i };
                    writer.AddRow(row);
                    demand.Add(1);
                    probes.Add(row);
                    if (writer.WrittenBytes * limits.WrittenSlack + (long)writer.WrittenCount * rowBytes >= limits.ByteCap)
                    {
                        break;
                    }
                }
                var estimate = writer.WrittenBytes * limits.WrittenSlack + (long)writer.WrittenCount * rowBytes;
                var written = writer.WrittenCount;
                var writtenBytes = writer.WrittenBytes;
                using (var stored = writer.GetRecordBatch())
                {
                    // The builders and their copy are both live here
                    var copy = GC.GetTotalMemory(true) - managedBefore + NativeAllocated() - nativeBefore;
                    // As the sink does, the builders go before matching
                    writer.NewBatch();
                    var batch = new DeleteBatch(stored, demand, comparers, changeData, false, prefilterColumn: 0);
                    // Nothing native is freed within a run, so allocated is live
                    var matching = GC.GetTotalMemory(true) - managedBefore + NativeAllocated() - nativeBefore;
                    GC.KeepAlive(batch);
                    GC.KeepAlive(probes);
                    GC.KeepAlive(demand);
                    GC.KeepAlive(writer);
                    if (run > 0)
                    {
                        samples.Add((written, writtenBytes / 1048576.0, estimate / 1048576.0, copy / 1048576.0, matching / 1048576.0));
                    }
                }
            }
            var rows = samples[0].Rows;
            var peak = samples.Select(x => Math.Max(x.Copy, x.Matching)).ToList();
            var perRow = ProbeKit.Median(peak) * 1048576.0 / rows;
            kit.Write($"rows {rows} | written {ProbeKit.Spread(samples.Select(x => x.Written).ToList())} MiB | estimate {ProbeKit.Spread(samples.Select(x => x.Estimate).ToList())} MiB" +
                $" | at the copy {ProbeKit.Spread(samples.Select(x => x.Copy).ToList())} MiB | matching {ProbeKit.Spread(samples.Select(x => x.Matching).ToList())} MiB" +
                $" | peak / estimate {ProbeKit.Median(peak) / ProbeKit.Median(samples.Select(x => x.Estimate)):0.00} | peak / 128 MiB {ProbeKit.Median(peak) / 128:0.00}" +
                $" | copy / written {ProbeKit.Median(samples.Select(x => x.Copy)) / ProbeKit.Median(samples.Select(x => x.Written)):0.00}" +
                $" | {perRow:0} B per row, {Math.Min(rowCap, 128.0 * 1048576 / perRow):0} rows at 128 MiB");
        }

        private static long NativeAllocated() => Apache.Arrow.Memory.MemoryAllocator.Default.Value.Statistics.BytesAllocated;

        // The sink's one order at a hit, the other rows a nanosecond past their millisecond
        private static async Task<byte[]> WorstCaseFile(ProbeTable table, string name, int rows, int hit)
        {
            using var ours = await ForeignRewrite.ReadOnlyFile(table.Storage, name);
            var key = ((Int64Array)ours.Column(0)).GetValue(0)!.Value;
            var millis = ((TimestampArray)ours.Column(1)).Values[0];
            var keys = new long[rows];
            var timestamps = new ParquetSharp.Int96[rows];
            for (int i = 0; i < rows; i++)
            {
                keys[i] = i == hit ? key : -1 - i;
                timestamps[i] = ForeignRewrite.Int96(millis, i == hit ? 0 : 1);
            }
            return ForeignRewrite.WriteInt96(ours.Schema.GetFieldByIndex(0).Name, ours.Schema.GetFieldByIndex(1).Name, keys, timestamps, rows);
        }

    }
}
