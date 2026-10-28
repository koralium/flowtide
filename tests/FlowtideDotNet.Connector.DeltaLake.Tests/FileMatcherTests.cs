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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class FileMatcherTests
    {
        private static readonly Delta.StructType Maps = new Delta.StructType(new[] { Field("m", new Delta.MapType(new Delta.StringType(), new Delta.LongType(), true)) });

        private static readonly Delta.StructType Longs = new Delta.StructType(new[] { Field("a", new Delta.LongType()), Field("b", new Delta.StringType()) });

        [Fact]
        public async Task ARepeatedKeyMapClaimsOnlyItsOwnRow()
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = await WriteFile(storage, Maps, "own.parquet", Rows(Map(("k", 1), ("k", 2)), Map(("k", 2), ("k", 1)), Map(("k", 3), ("k", 2))));

            var residual = await Match(storage, Maps, run, Rows(Map(("k", 1), ("k", 2))), new long[] { 1 }, 0);

            Assert.Equal(0, residual);
            Assert.Equal(new long[] { 0 }, run.Claims[0]);
            // Only its own row is a hit
            Assert.Equal(1, run.Statistics.Hits);
        }

        [Fact]
        public async Task ACollapsedForeignMapClaimsNeither()
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            var run = new DeleteRun(storage, "/t");
            // Another engine kept the last value of the repeated key
            var collapsed = Stored(Maps, Rows(Map(("k", 2)), Map(("k", 2))));
            await WriteBytes(storage, "/t/foreign.parquet", ForeignRewrite.Write(collapsed.Schema, new[] { collapsed.Column(0) }));
            run.Records[0] = new DeltaFileRecord() { Path = "foreign.parquet", PartitionValues = new Dictionary<string, string>() };

            var residual = await Match(storage, Maps, run, Rows(Map(("k", 1), ("k", 2))), new long[] { 1 }, 0);

            Assert.Equal(1, residual);
            Assert.Empty(run.Claims);
            Assert.Equal(2, run.Statistics.RowsHashed);
            Assert.Equal(0, run.Statistics.Hits);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task TwoPassesClaimEachPositionOnceAndLeaveTheExactResidual(bool collide, bool prefilter)
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory("./" + ScratchOwners.ShortId()));
            var run = new DeleteRun(storage, "/t");
            // File 0 holds (1,x) twice and (2,y), file 1 holds (1,x) once more; position 0 of file 0 is already deleted
            run.Records[0] = await WriteFile(storage, Longs, "zero.parquet", Rows(Row(1, "x"), Row(2, "y"), Row(1, "x"), Row(3, "z")), existing: new long[] { 0 });
            run.Records[1] = await WriteFile(storage, Longs, "one.parquet", Rows(Row(4, "w"), Row(1, "x")));
            storage.ClearRequests();

            // Three copies of (1,x) are asked for and two are live, then (2,y) and (1,x) once more
            var first = await Match(storage, Longs, run, Rows(Row(1, "x"), Row(2, "y"), Row(1, "x")), new long[] { 2, 1, 1 }, new[] { 0, 1 }, collide, prefilter);
            var second = await Match(storage, Longs, run, Rows(Row(2, "y"), Row(1, "x"), Row(9, "q")), new long[] { 1, 1, 1 }, new[] { 0, 1 }, collide, prefilter);

            Assert.Equal(1, first);
            Assert.Equal(3, second);
            Assert.Equal(new long[] { 0, 1, 2 }, run.Claims[0]);
            Assert.Equal(new long[] { 1 }, run.Claims[1]);
            // Each file is opened once per pass, its vector read once
            Assert.Equal(4, storage.Requests.Count(x => x.EndsWith(".parquet")));
            Assert.Equal(1, storage.Requests.Count(x => x.Contains("deletion_vector")));
        }

        [Fact]
        public async Task ACandidateWithoutAHitReadsNoVector()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory("./" + ScratchOwners.ShortId()));
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = await WriteFile(storage, Longs, "miss.parquet", Rows(Row(5, "v")), existing: new long[] { 0 });
            run.Records[1] = await WriteFile(storage, Longs, "hit.parquet", Rows(Row(1, "x")));
            storage.ClearRequests();

            var residual = await Match(storage, Longs, run, Rows(Row(1, "x")), new long[] { 1 }, 0, 1);

            Assert.Equal(0, residual);
            Assert.Equal(new long[] { 0 }, run.Claims[1]);
            Assert.DoesNotContain(storage.Requests, x => x.Contains("deletion_vector"));
            Assert.Equal(0, run.Statistics.VectorsRead);
        }

        [Fact]
        public async Task ClaimFreeVectorPeakIncludesVectorsClaimedLaterInThePass()
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = await WriteFile(storage, Longs, "zero.parquet", Rows(Row(1, "a")), existing: new long[] { 0 });
            run.Records[1] = await WriteFile(storage, Longs, "one.parquet", Rows(Row(2, "b"), Row(1, "a"), Row(1, "a")), existing: new long[] { 0 });
            run.Records[2] = await WriteFile(storage, Longs, "two.parquet", Rows(Row(1, "a")), existing: new long[] { 0 });
            long zero = run.Records[0].DeletionVector!.SizeInBytes;
            long one = run.Records[1].DeletionVector!.SizeInBytes;
            long two = run.Records[2].DeletionVector!.SizeInBytes;

            // The first pass retains only file one's claim-free vector
            var first = await Match(storage, Longs, run, Rows(Row(2, "b")), new long[] { 1 }, 0, 1);
            Assert.Equal(1, first);
            Assert.Empty(run.Claims);
            Assert.Equal(one, run.ClaimFreeVectorBytes);
            Assert.Equal(one, run.Statistics.PeakClaimFreeVectorBytes);

            using var stored = Stored(Longs, Rows(Row(1, "a")));
            var batch = new DeleteBatch(stored, new long[] { 3 }, Comparers(Longs));
            var matcher = new FileMatcher(storage, "/t", "t", Longs, false);
            await matcher.Scan(0, batch, run);
            Assert.Equal(zero + one, run.ClaimFreeVectorBytes);
            Assert.Empty(run.Claims);

            // File one's two live copies make its vector claimful once
            await matcher.Scan(1, batch, run);
            Assert.Equal(new long[] { 0, 1, 2 }, run.Claims[1]);
            Assert.Equal(zero, run.ClaimFreeVectorBytes);

            // A vector read after the claim, the peak is not a sum
            await matcher.Scan(2, batch, run);
            Assert.Equal(1, batch.Unresolved);
            Assert.Equal(zero + two, run.ClaimFreeVectorBytes);
            Assert.Equal(Math.Max(zero + one, zero + two), run.Statistics.PeakClaimFreeVectorBytes);
        }

        [Fact]
        public async Task AResolvedBatchOpensNoMoreFiles()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory("./" + ScratchOwners.ShortId()));
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = await WriteFile(storage, Longs, "first.parquet", Rows(Row(1, "x")));
            run.Records[1] = await WriteFile(storage, Longs, "second.parquet", Rows(Row(1, "x")));
            storage.ClearRequests();

            await Match(storage, Longs, run, Rows(Row(1, "x")), new long[] { 1 }, 0, 1);

            Assert.Equal(new[] { "first.parquet" }, storage.Requests.Where(x => x.EndsWith(".parquet")).Select(x => x.Split('/').Last()));
        }

        [Fact]
        public void EqualStoredRowsShareAnEntry()
        {
            var comparers = Comparers(Longs);
            using var stored = Stored(Longs, Rows(Row(1, "x"), Row(2, "y"), Row(1, "x"), Row(1, "x")));

            var batch = new DeleteBatch(stored, new long[] { 1, 2, 3, 4 }, comparers, changeData: true);
            batch.Claim(0);
            batch.Claim(0);

            Assert.Equal(2, batch.Count);
            Assert.Equal(8, batch.Unresolved);
            Assert.Equal(6, batch.Remaining(0));
            Assert.Equal(2, batch.Matched(0));
            Assert.Equal(0, batch.Matched(1));
        }

        [Theory]
        [InlineData(false, 300)]
        [InlineData(true, 1)]
        public async Task ThePrefilterHashesOnlyRowsThatPass(bool prefilter, int hashed)
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            var run = new DeleteRun(storage, "/t");
            // Three row groups of 100 rows, keys 0 to 299
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("a", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Field("b", Apache.Arrow.Types.StringType.Default, true))
                .Build();
            using (var source = new RecordBatch(schema, new IArrowArray[]
            {
                new Int64Array.Builder().AppendRange(Enumerable.Range(0, 300).Select(x => (long)x)).Build(),
                new StringArray.Builder().AppendRange(Enumerable.Range(0, 300).Select(x => $"v{x}")).Build()
            }, 300))
            using (var stream = new MemoryStream())
            {
                using (var writer = new ParquetSharp.Arrow.FileWriter(stream, schema, leaveOpen: true))
                {
                    writer.WriteRecordBatch(source, 100);
                    writer.Close();
                }
                await WriteBytes(storage, "/t/groups.parquet", stream.ToArray());
            }
            run.Records[0] = new DeltaFileRecord() { Path = "groups.parquet", PartitionValues = new Dictionary<string, string>() };

            var residual = await Match(storage, Longs, run, Rows(Row(250, "v250")), new long[] { 1 }, new[] { 0 }, false, prefilter);

            Assert.Equal(0, residual);
            Assert.Equal(new long[] { 250 }, run.Claims[0]);
            Assert.Equal(hashed, run.Statistics.RowsHashed);
            Assert.Equal(300, run.Statistics.RowsScanned);
        }

        [Fact]
        public async Task AMissingPrefilterColumnIsNull()
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            var run = new DeleteRun(storage, "/t");
            // Another engine's file without column a
            var only = new Apache.Arrow.Schema.Builder().Field(new Field("b", Apache.Arrow.Types.StringType.Default, true)).Build();
            await WriteBytes(storage, "/t/foreign.parquet", ForeignRewrite.Write(only, new IArrowArray[] { new StringArray.Builder().Append("x").Append("y").Build() }));
            run.Records[0] = new DeltaFileRecord() { Path = "foreign.parquet", PartitionValues = new Dictionary<string, string>() };

            Assert.Equal(1, await Match(storage, Longs, run, Rows(Row(1, "y")), new long[] { 1 }, new[] { 0 }, false, true));
            Assert.Empty(run.Claims);
            Assert.Equal(0, await Match(storage, Longs, run, Rows(new IDataValue[] { NullValue.Instance, new StringValue("y") }), new long[] { 1 }, new[] { 0 }, false, true));
            Assert.Equal(new long[] { 1 }, run.Claims[0]);
        }

        [Fact]
        public void APrefilterSetNeverMissesAnAddedValue()
        {
            var random = new Random(17);
            foreach (var count in new[] { 1, PrefilterSet.ExactLimit, 20_000 })
            {
                var values = Enumerable.Range(0, count).Select(_ => (ulong)random.NextInt64()).ToHashSet();
                var set = new PrefilterSet(values.ToArray());
                Assert.All(values, x => Assert.True(set.MayContain(x)));
                // About 1 % passes the filter, none the exact table
                var others = Enumerable.Range(0, 100_000).Select(_ => (ulong)random.NextInt64()).Where(x => !values.Contains(x)).Count(set.MayContain);
                Assert.InRange(others, 0, count > PrefilterSet.ExactLimit ? 2_000 : 0);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(PrefilterSet.ExactLimit)]
        [InlineData(PrefilterSet.ExactLimit + 1)]
        public void RepeatedPrefilterValuesKeepTheExactSet(int rows)
        {
            using var stored = Stored(Longs, Enumerable.Range(0, rows).Select(i => Row(0, $"v{i}")).ToList());
            var batch = new DeleteBatch(stored, Enumerable.Repeat(1L, rows).ToArray(), Comparers(Longs), prefilterColumn: 0);
            Assert.Equal(rows, batch.Count);

            // Every distinct row has the same first-column hash
            ulong present = 0;
            Assert.True(new ArrowInt64Comparer().TryHash(stored.Column(0), 0, ref present));
            Assert.Equal(0UL, present);
            Assert.True(batch.MayMatch(present));
            // These hashes share Bloom positions, but are distinct
            Assert.False(batch.MayMatch(1UL << 32));
        }

        [Fact]
        public void BatchHashesAreTheRowHashes()
        {
            using var stored = Stored(Longs, Rows(Row(1, "x"), Row(2, "y"), new IDataValue[] { NullValue.Instance, new StringValue("z") }, new IDataValue[] { new Int64Value(4), NullValue.Instance }));
            var batch = new DeleteBatch(stored, new long[] { 1, 1, 1, 1 }, Comparers(Longs));
            var columns = new[] { stored.Column(0), stored.Column(1) };
            var hashes = new ulong[4];
            var rejected = new bool[4];

            batch.HashBatch(columns, hashes, rejected);

            for (int row = 0; row < 4; row++)
            {
                Assert.True(batch.TryHash(columns, row, out var hash));
                Assert.False(rejected[row]);
                Assert.Equal(hash, hashes[row]);
                Assert.Equal(row, batch.Find(hashes[row], columns, row));
            }
        }

        // Scans the files in order and returns the copies not found
        private static async Task<long> Match(IFileStorage storage, Delta.StructType table, DeleteRun run, List<IDataValue[]> rows, long[] demand, params int[] files)
        {
            return await Match(storage, table, run, rows, demand, files, false, false);
        }

        private static async Task<long> Match(IFileStorage storage, Delta.StructType table, DeleteRun run, List<IDataValue[]> rows, long[] demand, int[] files, bool collide, bool prefilter)
        {
            using var stored = Stored(table, rows);
            var batch = new DeleteBatch(stored, demand, Comparers(table), collide: collide, prefilterColumn: prefilter ? 0 : -1);
            var matcher = new FileMatcher(storage, "/t", "t", table, false);
            foreach (var id in files)
            {
                if (batch.Unresolved == 0)
                {
                    break;
                }
                await matcher.Scan(id, batch, run);
            }
            return batch.Unresolved;
        }

        private static IArrowComparer[] Comparers(Delta.StructType table)
        {
            var visitor = new ArrowComparerVisitor();
            return table.Fields.Select(x => x.Type.Accept(visitor)).ToArray();
        }

        // The rows as the delete writer stores them
        private static RecordBatch Stored(Delta.StructType table, List<IDataValue[]> rows)
        {
            using var data = Batch(table, rows);
            var writer = new ParquetSharpWriter(table, table.Fields.Select(x => x.Name).ToList());
            writer.NewBatch();
            for (int i = 0; i < rows.Count; i++)
            {
                writer.AddRow(new ColumnRowReference() { referenceBatch = data, RowIndex = i });
            }
            return writer.GetRecordBatch();
        }

        private static async Task<DeltaFileRecord> WriteFile(IFileStorage storage, Delta.StructType table, string path, List<IDataValue[]> rows, long[]? existing = null)
        {
            using var data = Batch(table, rows);
            var writer = new ParquetSharpWriter(table, table.Fields.Select(x => x.Name).ToList());
            writer.NewBatch();
            for (int i = 0; i < rows.Count; i++)
            {
                writer.AddRow(new ColumnRowReference() { referenceBatch = data, RowIndex = i });
            }
            var size = await writer.WriteData(storage, "/t", path);
            DeletionVector? vector = null;
            if (existing != null)
            {
                var bitmap = RoaringBitmapArray.Create(existing);
                var (vectorPath, z85) = DeletionVectorWriter.GenerateDestination();
                var (_, dataSize) = await DeletionVectorWriter.WriteDeletionVector(storage, "/t", vectorPath, bitmap);
                vector = new DeletionVector() { Cardinality = bitmap.Cardinality, Offset = 1, StorageType = "u", PathOrInlineDv = z85, SizeInBytes = dataSize };
            }
            return new DeltaFileRecord() { Path = path, Size = size, PartitionValues = new Dictionary<string, string>(), DeletionVector = vector };
        }

        private static EventBatchData Batch(Delta.StructType table, List<IDataValue[]> rows)
        {
            var columns = table.Fields.Select(_ => FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance)).ToArray();
            foreach (var row in rows)
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c].Add(row[c]);
                }
            }
            return new EventBatchData(columns.Cast<IColumn>().ToArray());
        }

        private static async Task WriteBytes(IFileStorage storage, string path, byte[] bytes)
        {
            using var write = await storage.OpenWrite(path);
            await write.WriteAsync(bytes);
        }

        private static List<IDataValue[]> Rows(params IDataValue[][] rows) => rows.ToList();

        private static IDataValue[] Row(long a, string b) => new IDataValue[] { new Int64Value(a), new StringValue(b) };

        private static IDataValue[] Map(params (string Key, long Value)[] entries)
        {
            return new IDataValue[] { new MapValue(entries.Select(x => new KeyValuePair<IDataValue, IDataValue>(new StringValue(x.Key), new Int64Value(x.Value))).ToArray()) };
        }

        private static Delta.StructField Field(string name, FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type)
        {
            return new Delta.StructField(name, type, true, new Dictionary<string, object>());
        }
    }
}
