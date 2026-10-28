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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using Stowage;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class WriterOutputTests
    {
        [Fact]
        public async Task AnInsertedNullMapReadsBackNull()
        {
            var testName = nameof(AnInsertedNullMapReadsBackNull);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    m MAP<STRING, STRING>
                );

                INSERT INTO test
                SELECT userkey, CASE WHEN userkey % 2 = 0 THEN map('a', firstName) END as m FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            await AssertReaderResult(testName, storage, "test", stream.Users.Select(x => new
            {
                x.UserKey,
                m = x.UserKey % 2 == 0 ? new Dictionary<string, string>() { ["a"] = x.FirstName! } : null
            }));
        }

        [Fact]
        public async Task AnOmittedNullableStructColumnReadsNull()
        {
            var testName = nameof(AnOmittedNullableStructColumnReadsNull);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(
                "{\"type\":\"struct\",\"fields\":[" +
                "{\"name\":\"userkey\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                "{\"name\":\"s\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO test
                SELECT userkey FROM users
            ");
            await WaitForVersion(storage, "test", stream, 1);

            await AssertReaderResult(testName, storage, "test", stream.Users.Select(x => new { x.UserKey, s = default(object) }));
        }

        [Fact]
        public async Task ANullStructAndAStructOfNullsStayApart()
        {
            var testName = nameof(ANullStructAndAStructOfNullsStayApart);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.WriteChangeDataOnNewTables = true);
            stream.Generate(10);

            // Even keys hold a struct of nulls, odd keys a NULL struct
            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    s STRUCT<a STRING>
                );

                INSERT INTO test
                SELECT userkey, CASE WHEN userkey % 2 = 0 THEN named_struct('a', CASE WHEN userkey < 0 THEN firstName END) END as s FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var initial = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0);
            var data = await ReadBytes(storage, "/test/" + Assert.Single(initial!.AddedFiles).Path);
            Assert.All(await NullStructs(data), x => Assert.Equal(x.Key % 2 != 0, x.Null));
            await AssertReaderResult(testName, storage, "test", stream.Users.Select(x => new { x.UserKey, s = x.UserKey % 2 == 0 ? new { a = default(string) } : null }));

            // The change data file keeps the same validity
            stream.DeleteUser(stream.Users.First(x => x.UserKey % 2 == 0));
            stream.DeleteUser(stream.Users.First(x => x.UserKey % 2 != 0));
            await WaitForVersion(storage, "test", stream, 1);
            var change = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 1);
            var deletes = await NullStructs(await ReadBytes(storage, "/test/" + Assert.Single(change!.CdcFiles).Path));
            Assert.Equal(2, deletes.Count);
            Assert.All(deletes, x => Assert.Equal(x.Key % 2 != 0, x.Null));
        }

        [Fact]
        public async Task WrittenTimestampsAreAdjustedToUtc()
        {
            var testName = nameof(WrittenTimestampsAreAdjustedToUtc);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.WriteChangeDataOnNewTables = true);
            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    orderkey INT,
                    orderdate TIMESTAMP
                );

                INSERT INTO test
                SELECT orderkey, orderdate FROM orders
            ");
            await WaitForVersion(storage, "test", stream, 0);
            stream.DeleteOrder(stream.Orders[0]);
            await WaitForVersion(storage, "test", stream, 1);

            var initial = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0);
            var change = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 1);
            Assert.Equal((ParquetSharp.TimeUnit.Millis, true), RewriteCopyTests.TimestampLeaf(await ReadBytes(storage, "/test/" + Assert.Single(initial!.AddedFiles).Path), 1));
            Assert.Equal((ParquetSharp.TimeUnit.Millis, true), RewriteCopyTests.TimestampLeaf(await ReadBytes(storage, "/test/" + Assert.Single(change!.CdcFiles).Path), 1));
        }

        [Fact]
        public async Task AColumnMappedInsertWritesPhysicalNestedNamesAndIds()
        {
            var testName = nameof(AColumnMappedInsertWritesPhysicalNestedNamesAndIds);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.EnableColumnMappingOnNewTables = true);
            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    s STRUCT<firstName STRING, lastName STRING>
                );

                INSERT INTO test
                SELECT userkey, named_struct('firstName', firstName, 'lastName', lastName) as s FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var schema = (await DeltaTransactionReader.ReadTable(storage, "test"))!.Schema;
            var userkey = schema.Fields[0];
            var s = schema.Fields[1];
            var nested = ((Internal.Delta.Schema.Types.StructType)s.Type).Fields;
            Assert.All(nested, x => Assert.NotNull(x.PhysicalName));
            var add = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles);

            Assert.Equal($"{userkey.PhysicalName}:{userkey.FieldId},{s.PhysicalName}:{s.FieldId}({string.Join(",", nested.Select(x => $"{x.PhysicalName}:{x.FieldId}"))})",
                RewriteCopyTests.Footer(await ReadBytes(storage, "/test/" + add.Path)));
            using var stats = JsonDocument.Parse(add.Statistics!);
            foreach (var bound in new[] { "minValues", "maxValues", "nullCount" })
            {
                Assert.Equal(nested.Select(x => x.PhysicalName!).OrderBy(x => x), stats.RootElement.GetProperty(bound).GetProperty(s.PhysicalName!).EnumerateObject().Select(x => x.Name).OrderBy(x => x));
            }
            await AssertReaderResult(testName, storage, "test", stream.Users.Select(x => new { x.UserKey, s = new { firstName = x.FirstName, lastName = x.LastName } }));
        }

        [Fact]
        public async Task AnUnmappedInsertKeepsItsNames()
        {
            var testName = nameof(AnUnmappedInsertKeepsItsNames);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.EnableColumnMappingOnNewTables = false);
            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    s STRUCT<firstName STRING, lastName STRING>
                );

                INSERT INTO test
                SELECT userkey, named_struct('firstName', firstName, 'lastName', lastName) as s FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var add = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles);
            Assert.Equal("userkey:-1,s:-1(firstName:-1,lastName:-1)", RewriteCopyTests.Footer(await ReadBytes(storage, "/test/" + add.Path)));
            using var stats = JsonDocument.Parse(add.Statistics!);
            Assert.Equal(new[] { "firstName", "lastName" }, stats.RootElement.GetProperty("minValues").GetProperty("s").EnumerateObject().Select(x => x.Name));
        }

        [Theory]
        [InlineData("named_struct('lastName', lastName, 'firstName', firstName)")]
        [InlineData("map('lastName', lastName, 'firstName', firstName)")]
        public async Task AStructOutOfKeyOrderKeepsItsValues(string value)
        {
            var testName = nameof(AStructOutOfKeyOrderKeepsItsValues) + (value.StartsWith("map") ? "_map" : "_struct");
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);

            await stream.StartStream($@"
                CREATE TABLE test (
                    userkey INT,
                    s STRUCT<lastName STRING, firstName STRING>
                );

                INSERT INTO test
                SELECT userkey, {value} as s FROM users
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var add = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles);
            var rows = await RewriteCopyTests.ReadRows(await ReadBytes(storage, "/test/" + add.Path), batch => Enumerable.Range(0, batch.Length).Select(i =>
            {
                var s = (StructArray)batch.Column(1);
                return $"{((Int64Array)batch.Column(0)).GetValue(i)} {((StringArray)s.Fields[0]).GetString(i)} {((StringArray)s.Fields[1]).GetString(i)}";
            }));
            Assert.Equal(stream.Users.Select(x => $"{x.UserKey} {x.LastName} {x.FirstName}").OrderBy(x => x), rows.OrderBy(x => x));
        }

        // Each row's key and whether its struct is null
        private static Task<List<(int Key, bool Null)>> NullStructs(byte[] file)
        {
            return RewriteCopyTests.ReadRows(file, batch => Enumerable.Range(0, batch.Length).Select(i => ((int)((Int64Array)batch.Column(0)).GetValue(i)!.Value, batch.Column(1).IsNull(i))));
        }
    }
}
