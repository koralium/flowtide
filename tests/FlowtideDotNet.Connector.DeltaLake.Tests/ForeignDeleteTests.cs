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
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ForeignDeleteTests
    {
        private const string Orders = @"
            CREATE TABLE test (
                orderkey INT,
                orderdate TIMESTAMP
            );

            INSERT INTO test
            SELECT orderkey, orderdate FROM orders
        ";

        [Theory]
        [InlineData("micros")]
        [InlineData("nanos")]
        [InlineData("int96")]
        public async Task RowsAreDeletedFromAForeignTimestampRewrite(string unit)
        {
            var testName = nameof(RowsAreDeletedFromAForeignTimestampRewrite) + unit;
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            var orders = await ReadOrders(storage);
            await ForeignRewrite.Commit(storage, "test", WriteOrders(orders, unit, orders.Keys, orders.Millis, new long[orders.Keys.Length]));
            foreach (var order in stream.Orders.Take(3).ToList())
            {
                stream.DeleteOrder(order);
            }
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Orders.Select(x => x.OrderKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).Order());
        }

        [Theory]
        [InlineData("micros", 500_000)]
        [InlineData("nanos", 1)]
        [InlineData("int96", 500)]
        public async Task ACopyFinerThanAMillisecondIsNotClaimed(string unit, long pastNanos)
        {
            var testName = nameof(ACopyFinerThanAMillisecondIsNotClaimed) + unit;
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            // The finer copy comes first in the file
            var orders = await ReadOrders(storage);
            var deleted = stream.Orders[0];
            var index = System.Array.IndexOf(orders.Keys, (long)deleted.OrderKey);
            var keys = orders.Keys.Prepend((long)deleted.OrderKey).ToArray();
            var millis = orders.Millis.Prepend(orders.Millis[index]).ToArray();
            var extra = new long[keys.Length];
            extra[0] = pastNanos;
            await ForeignRewrite.Commit(storage, "test", WriteOrders(orders, unit, keys, millis, extra));
            stream.DeleteOrder(deleted);
            await WaitForVersion(storage, "test", stream, 2);

            var live = await ForeignRewrite.LiveRows(storage, "test");
            Assert.Equal(new long[] { 0 }, live.Where(x => Key(x.Row) == deleted.OrderKey).Select(x => x.Position));
            Assert.Equal(keys.Length - 1, live.Count);
        }

        [Fact]
        public async Task AnInt96RowOnTheFirstDayIsDeleted()
        {
            var testName = nameof(AnInt96RowOnTheFirstDayIsDeleted);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            var first = new Order() { OrderKey = 1_000_000, UserKey = stream.Users[0].UserKey, Orderdate = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
            stream.AddOrder(first);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            var orders = await ReadOrders(storage);
            Assert.Contains(-62_135_596_800_000L, orders.Millis);
            await ForeignRewrite.Commit(storage, "test", WriteOrders(orders, "int96", orders.Keys, orders.Millis, new long[orders.Keys.Length]));
            stream.DeleteOrder(first);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Orders.Select(x => x.OrderKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).Order());
        }

        [Fact]
        public async Task HitsInLaterInt96RowGroupsClaimTheRightRows()
        {
            var testName = nameof(HitsInLaterInt96RowGroupsClaimTheRightRows);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            // Four row groups of 70k rows, read in batches of 64k; rows of groups 0 and 1 are finer than a microsecond
            const int groupRows = 70_000;
            var orders = await ReadOrders(storage);
            var first = stream.Orders[0];
            var second = stream.Orders[1];
            var keys = new long[4 * groupRows];
            var millis = new long[keys.Length];
            var extra = new long[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = -1 - i;
                extra[i] = i < 2 * groupRows ? 1 : 0;
            }
            Place(orders, first.OrderKey, 2 * groupRows + 10, keys, millis);
            Place(orders, second.OrderKey, 3 * groupRows + 10, keys, millis);
            // A finer copy before each exact copy
            Place(orders, first.OrderKey, 2 * groupRows + 5, keys, millis);
            Place(orders, second.OrderKey, 3 * groupRows + 5, keys, millis);
            extra[2 * groupRows + 5] = 500;
            extra[3 * groupRows + 5] = 500;
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.WriteInt96(orders.KeyName, orders.DateName, keys, millis.Select((x, i) => ForeignRewrite.Int96(x, extra[i])).ToArray(), groupRows));
            stream.DeleteOrder(first);
            stream.DeleteOrder(second);
            await WaitForVersion(storage, "test", stream, 2);

            var live = await ForeignRewrite.LiveRows(storage, "test");
            Assert.Equal(keys.Length - 2, live.Count);
            Assert.DoesNotContain(live, x => x.Position == 2 * groupRows + 10 || x.Position == 3 * groupRows + 10);
            Assert.Contains(live, x => x.Position == 2 * groupRows + 5);
            Assert.Contains(live, x => x.Position == 3 * groupRows + 5);
            // Only the row groups with a hit are read raw
            Assert.Equal(2, stream.Sinks[0].DeleteStatisticsForTests!.RowGroupsRawRead);
        }

        [Fact]
        public async Task HitsInOneInt96RowGroupAreReadForward()
        {
            var testName = nameof(HitsInOneInt96RowGroupAreReadForward);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            // One row group over several Arrow batches, a copy 500 ns later just before each order
            const int rows = 200_000;
            var positions = new[] { 1_000, 70_000, 140_000 };
            var orders = await ReadOrders(storage);
            var deleted = stream.Orders.Take(positions.Length).ToList();
            var keys = new long[rows];
            var millis = new long[rows];
            var extra = new long[rows];
            for (int i = 0; i < rows; i++)
            {
                keys[i] = -1 - i;
            }
            for (int i = 0; i < positions.Length; i++)
            {
                Place(orders, deleted[i].OrderKey, positions[i], keys, millis);
                Place(orders, deleted[i].OrderKey, positions[i] - 1, keys, millis);
                extra[positions[i] - 1] = 500;
            }
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.WriteInt96(orders.KeyName, orders.DateName, keys, millis.Select((x, i) => ForeignRewrite.Int96(x, extra[i])).ToArray(), rows));
            foreach (var order in deleted)
            {
                stream.DeleteOrder(order);
            }
            await WaitForVersion(storage, "test", stream, 2);

            var live = (await ForeignRewrite.LiveRows(storage, "test")).Select(x => x.Position).ToHashSet();
            Assert.Equal(rows - positions.Length, live.Count);
            Assert.All(positions, x => Assert.DoesNotContain((long)x, live));
            var statistics = stream.Sinks[0].DeleteStatisticsForTests!;
            Assert.Equal(1, statistics.RowGroupsRawRead);
            // The last hit's batch of levels and one more, never the whole group
            Assert.InRange(statistics.RawLevelsRead, positions[^1] + 1, positions[^1] + 2 * 4096);
        }

        [Fact]
        public async Task NestedForeignMicrosecondsAreDeleted()
        {
            var testName = nameof(NestedForeignMicrosecondsAreDeleted);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    s STRUCT<t TIMESTAMP>,
                    l LIST<TIMESTAMP>,
                    m MAP<TIMESTAMP, STRING>
                );

                INSERT INTO test
                SELECT
                    userkey,
                    named_struct('t', timestamp_parse('2020-01-02 03:04:05.006', 'yyyy-MM-dd HH:mm:ss.fff')) as s,
                    list(timestamp_parse('2021-01-02 03:04:05.007', 'yyyy-MM-dd HH:mm:ss.fff')) as l,
                    map(timestamp_parse('2022-01-02 03:04:05.008', 'yyyy-MM-dd HH:mm:ss.fff'), firstName) as m
                FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var source = await ForeignRewrite.ReadOnlyFile(storage, "test");
            var columns = Enumerable.Range(0, source.ColumnCount).Select(i => i == 0 ? source.Column(0) : ForeignRewrite.Micros(source.Column(i))).ToArray();
            var schema = new Apache.Arrow.Schema(columns.Select((x, i) => new Field(source.Schema.GetFieldByIndex(i).Name, x.Data.DataType, true, source.Schema.GetFieldByIndex(i).Metadata)), null);
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.Write(schema, columns));
            for (int i = 1; i < source.ColumnCount; i++)
            {
                source.Column(i).Dispose();
            }
            Assert.Contains("Microsecond", (await ForeignRewrite.LiveRows(storage, "test"))[0].Row);
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Users.Select(x => x.UserKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).Order());
        }

        [Fact]
        public async Task AResolvedBatchOpensNoFurtherCandidate()
        {
            var testName = nameof(AResolvedBatchOpensNoFurtherCandidate);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            // Another writer added a copy of the file, so each row is there twice
            var own = Assert.Single((await DeltaTransactionReader.ReadTable(storage, "test"))!.AddFiles);
            var bytes = await ReadBytes(storage, $"/test/{own.Path}");
            using (var write = await storage.OpenWrite("/test/copy.parquet"))
            {
                await write.WriteAsync(bytes);
            }
            await WriteCommit(storage, "test", 1, new DeltaAction() { Add = new DeltaAddAction() { Path = "copy.parquet", Size = bytes.Length, DataChange = true, PartitionValues = new Dictionary<string, string>(), Statistics = own.Statistics } });
            storage.ClearRequests();
            var deleted = stream.Orders[0];
            stream.DeleteOrder(deleted);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("OpenRead ") && x.Contains("copy.parquet"));
            var keys = (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).ToList();
            Assert.Single(keys, deleted.OrderKey);
            Assert.Equal(2 * stream.Orders.Count + 1, keys.Count);
        }

        [Fact]
        public async Task ReorderedForeignColumnsAreDeleted()
        {
            var testName = nameof(ReorderedForeignColumnsAreDeleted);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(20);
            await stream.StartStream(Orders);
            await WaitForVersion(storage, "test", stream, 0);

            var source = await ForeignRewrite.ReadOnlyFile(storage, "test");
            var schema = new Apache.Arrow.Schema(new[] { source.Schema.GetFieldByIndex(1), source.Schema.GetFieldByIndex(0) }, null);
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.Write(schema, new[] { source.Column(1), source.Column(0) }));
            stream.DeleteOrder(stream.Orders[0]);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Orders.Select(x => x.OrderKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).Order());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ANullStructAndAStructOfNullsAreClaimedApart(bool foreign)
        {
            var testName = nameof(ANullStructAndAStructOfNullsAreClaimedApart) + foreign;
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(@"
                CREATE TABLE test (
                    s STRUCT<a STRING>
                );

                INSERT INTO test
                SELECT CASE WHEN userkey % 2 = 0 THEN named_struct('a', CASE WHEN userkey < 0 THEN firstName END) END as s FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);
            var version = 0;
            if (foreign)
            {
                var source = await ForeignRewrite.ReadOnlyFile(storage, "test");
                await ForeignRewrite.Commit(storage, "test", ForeignRewrite.Write(source.Schema, new[] { source.Column(0) }));
                version++;
            }

            stream.DeleteUser(stream.Users.First(x => x.UserKey % 2 == 0));
            await WaitForVersion(storage, "test", stream, version + 1);
            Assert.Equal(new[] { ("null", 5), ("{null}", 4) }, await CountRows(storage));
            stream.DeleteUser(stream.Users.First(x => x.UserKey % 2 != 0));
            await WaitForVersion(storage, "test", stream, version + 2);
            Assert.Equal(new[] { ("null", 4), ("{null}", 4) }, await CountRows(storage));
        }

        [Fact]
        public async Task PermutedForeignMapsAreDeleted()
        {
            var testName = nameof(PermutedForeignMapsAreDeleted);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    m MAP<STRING, STRING>
                );

                INSERT INTO test
                SELECT userkey, map('lastName', lastName, 'firstName', firstName) as m FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var source = await ForeignRewrite.ReadOnlyFile(storage, "test");
            var maps = (MapArray)source.Column(1);
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.Write(source.Schema, new[] { source.Column(0), ForeignRewrite.Reversed(maps) }));
            maps.Dispose();
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Users.Select(x => x.UserKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).Order());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PermutedForeignMapsWithCompositeKeysAreDeleted(bool mapKeys)
        {
            var testName = nameof(PermutedForeignMapsWithCompositeKeysAreDeleted) + mapKeys;
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            var type = mapKeys ? "MAP<STRING, STRING>" : "STRUCT<k STRING, n STRING>";
            var first = mapKeys ? "map('k', firstName, 'n', '1')" : "named_struct('k', firstName, 'n', '1')";
            var second = mapKeys ? "map('k', lastName, 'n', '2')" : "named_struct('k', lastName, 'n', '2')";
            await stream.StartStream($@"
                CREATE TABLE test (
                    userkey INT,
                    m MAP<{type}, STRING>
                );

                INSERT INTO test
                SELECT userkey, map({first}, firstName, {second}, lastName) as m FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var source = await ForeignRewrite.ReadOnlyFile(storage, "test");
            var maps = (MapArray)source.Column(1);
            Assert.All(Enumerable.Range(0, maps.Length), i => Assert.Equal(2, maps.GetValueLength(i)));
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.Write(source.Schema, new[] { source.Column(0), ForeignRewrite.Reversed(maps) }));
            maps.Dispose();
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(stream.Users.Select(x => x.UserKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => Key(x.Row)).Order());
        }

        [Fact]
        public async Task AFileWithLogicalNestedNamesIsRewrittenUnderPhysicalNames()
        {
            var testName = nameof(AFileWithLogicalNestedNamesIsRewrittenUnderPhysicalNames);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.EnableDeletionVectorsOnNewTables = false);
            stream.Generate(10);
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    s STRUCT<a STRING, n STRUCT<b STRING> >,
                    l LIST<STRUCT<c STRING> >,
                    m MAP<STRUCT<k STRING>, STRUCT<v STRING> >,
                    t TIMESTAMP
                );

                INSERT INTO test
                SELECT
                    userkey,
                    named_struct('a', firstName, 'n', named_struct('b', lastName)) as s,
                    list(named_struct('c', firstName)) as l,
                    map(named_struct('k', firstName), named_struct('v', lastName)) as m,
                    timestamp_parse('2020-01-02 03:04:05.006', 'yyyy-MM-dd HH:mm:ss.fff') as t
                FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var head = await DeltaTransactionReader.ReadTable(storage, "test");
            var source = await ForeignRewrite.ReadOnlyFile(storage, "test");
            var columns = Enumerable.Range(0, source.ColumnCount).Select(i => i == 0 ? source.Column(0) : ForeignRewrite.Logical(source.Column(i), head!.Schema, i)).ToArray();
            var schema = new Apache.Arrow.Schema(columns.Select((x, i) => new Field(source.Schema.GetFieldByIndex(i).Name, x.Data.DataType, true, source.Schema.GetFieldByIndex(i).Metadata)), null);
            await ForeignRewrite.Commit(storage, "test", ForeignRewrite.Write(schema, columns));
            var before = (await ForeignRewrite.LiveRows(storage, "test")).Select(x => x.Row).ToList();
            var deleted = stream.Users[0];
            stream.DeleteUser(deleted);
            await WaitForVersion(storage, "test", stream, 2);

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 2);
            var rewritten = Assert.Single(commit!.AddedFiles);
            Assert.Null(rewritten.DeletionVector);
            Assert.Equal(before.Where(x => Key(x) != deleted.UserKey).Order(), (await ForeignRewrite.LiveRows(storage, "test")).Select(x => x.Row).Order());
            Assert.Contains($"{deleted.UserKey} {{{deleted.FirstName},{{{deleted.LastName}}}}} [{{{deleted.FirstName}}}] {{{{{deleted.FirstName}}}:{{{deleted.LastName}}}}} ", before.Single(x => Key(x) == deleted.UserKey) + " ");
            var footer = LeafPathNames(await ReadBytes(storage, "/test/" + rewritten.Path));
            var nested = head!.Schema.Fields.Skip(1).SelectMany(x => NestedFields(x.Type)).ToList();
            Assert.Equal(6, nested.Count);
            Assert.All(nested, x => Assert.Contains(x.PhysicalName!, footer));
            Assert.All(nested, x => Assert.DoesNotContain(x.Name, footer));
        }

        // The sink's file: its column names, keys and milliseconds
        private sealed record OrderFile(string KeyName, string DateName, Field DateField, Field KeyField, long[] Keys, long[] Millis);

        private static async Task<OrderFile> ReadOrders(IFileStorage storage)
        {
            using var batch = await ForeignRewrite.ReadOnlyFile(storage, "test");
            var keys = (Int64Array)batch.Column(0);
            var dates = (TimestampArray)batch.Column(1);
            Assert.Equal(TimeUnit.Millisecond, ((TimestampType)dates.Data.DataType).Unit);
            return new OrderFile(batch.Schema.GetFieldByIndex(0).Name, batch.Schema.GetFieldByIndex(1).Name, batch.Schema.GetFieldByIndex(1), batch.Schema.GetFieldByIndex(0),
                keys.Values.ToArray(), dates.Values.ToArray());
        }

        // Each row is its milliseconds plus extra nanoseconds
        private static byte[] WriteOrders(OrderFile orders, string unit, long[] keys, long[] millis, long[] extraNanos)
        {
            if (unit == "int96")
            {
                return ForeignRewrite.WriteInt96(orders.KeyName, orders.DateName, keys, millis.Select((x, i) => ForeignRewrite.Int96(x, extraNanos[i])).ToArray(), keys.Length);
            }
            var timeUnit = unit == "micros" ? TimeUnit.Microsecond : TimeUnit.Nanosecond;
            var values = millis.Select((x, i) => unit == "micros" ? x * 1_000 + extraNanos[i] / 1_000 : x * 1_000_000 + extraNanos[i]).ToArray();
            var type = new TimestampType(timeUnit, "UTC");
            var schema = new Apache.Arrow.Schema(new[] { orders.KeyField, new Field(orders.DateName, type, true, orders.DateField.Metadata) }, null);
            return ForeignRewrite.Write(schema, new IArrowArray[]
            {
                new Int64Array.Builder().AppendRange(keys).Build(),
                new TimestampArray(type, new ArrowBuffer.Builder<long>().AppendRange(values).Build(), ArrowBuffer.Empty, values.Length, 0, 0)
            });
        }

        private static void Place(OrderFile orders, long key, int position, long[] keys, long[] millis)
        {
            keys[position] = key;
            millis[position] = orders.Millis[System.Array.IndexOf(orders.Keys, key)];
        }

        private static async Task<(string, int)[]> CountRows(IFileStorage storage)
        {
            return (await ForeignRewrite.LiveRows(storage, "test")).GroupBy(x => x.Row).Select(x => (x.Key, x.Count())).OrderBy(x => x.Key, StringComparer.Ordinal).ToArray();
        }

        // Every name on the path of a leaf
        private static HashSet<string> LeafPathNames(byte[] file)
        {
            using var reader = new ParquetSharp.ParquetFileReader(new MemoryStream(file));
            var schema = reader.FileMetaData.Schema;
            var names = new HashSet<string>();
            for (int i = 0; i < schema.NumColumns; i++)
            {
                using var path = schema.Column(i).Path;
                names.UnionWith(path.ToDotVector());
            }
            return names;
        }

        // Every struct field below the top level
        private static IEnumerable<Internal.Delta.Schema.Types.StructField> NestedFields(Internal.Delta.Schema.SchemaBaseType type)
        {
            return type switch
            {
                Internal.Delta.Schema.Types.StructType structType => structType.Fields.SelectMany(x => NestedFields(x.Type).Prepend(x)),
                Internal.Delta.Schema.Types.ArrayType list => NestedFields(list.ElementType!),
                Internal.Delta.Schema.Types.MapType map => NestedFields(map.KeyType).Concat(NestedFields(map.ValueType)),
                _ => Enumerable.Empty<Internal.Delta.Schema.Types.StructField>()
            };
        }

        private static int Key(string row)
        {
            return int.Parse(row.Split(' ')[0], System.Globalization.CultureInfo.InvariantCulture);
        }
    }
}
