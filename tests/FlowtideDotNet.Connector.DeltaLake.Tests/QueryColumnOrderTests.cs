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
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using Stowage;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class QueryColumnOrderTests
    {
        private const string Schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";

        private const string Reordered = @"
            CREATE TABLE t (a INT, b INT, c STRING);
            INSERT INTO t SELECT userKey + 1000 AS b, userKey AS a, firstName AS c FROM users";

        private const string Aligned = @"
            CREATE TABLE t (a INT, b INT, c STRING);
            INSERT INTO t SELECT userKey AS a, userKey + 1000 AS b, firstName AS c FROM users";

        [Fact]
        public async Task ReorderedQueryColumnsWriteCorrectFields()
        {
            var testName = nameof(ReorderedQueryColumnsWriteCorrectFields);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(Schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(Reordered);
            await WaitForVersion(storage, "t", stream, 1);

            var add = Assert.Single((await ReadCommitActions(storage, "t", 1)).Where(x => x.Add != null)).Add!;
            // Read by field name, without the Delta reader
            using var reader = ParquetFileReaders.Open(new MemoryStream(await ReadBytes(storage, $"/t/{add.Path}")));
            using var batches = reader.GetRecordBatchReader();
            using var batch = await batches.ReadNextRecordBatchAsync();
            var a = (Int32Array)batch.Column("a");
            var b = (Int32Array)batch.Column("b");
            var c = (StringArray)batch.Column("c");
            var rows = Enumerable.Range(0, batch.Length).Select(i => (a.GetValue(i), b.GetValue(i), c.GetString(i))).OrderBy(x => x.Item1);
            Assert.Equal(stream.Users.Select(x => ((int?)x.UserKey, (int?)x.UserKey + 1000, x.FirstName!)).OrderBy(x => x.Item1), rows);

            // Statistics name the fields that hold the values
            using var statistics = JsonDocument.Parse(add.Statistics!);
            Assert.Equal(stream.Users.Min(x => x.UserKey), statistics.RootElement.GetProperty("minValues").GetProperty("a").GetInt32());
            Assert.Equal(stream.Users.Max(x => x.UserKey) + 1000, statistics.RootElement.GetProperty("maxValues").GetProperty("b").GetInt32());

            await AssertReaderResult(testName, storage, "t", stream.Users.Select(x => new { a = x.UserKey, b = x.UserKey + 1000, c = x.FirstName }));
        }

        [Fact]
        public async Task ReorderedQueryColumnsDeleteTheirRows()
        {
            var testName = nameof(ReorderedQueryColumnsDeleteTheirRows);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(Schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(Reordered);
            await WaitForVersion(storage, "t", stream, 1);

            stream.DeleteUser(stream.Users[3]);
            await WaitForVersion(storage, "t", stream, 2);

            await AssertReaderResult(testName, storage, "t", stream.Users.Select(x => new { a = x.UserKey, b = x.UserKey + 1000, c = x.FirstName }));
        }

        [Fact]
        public async Task AlignedQueryColumnsWriteTheSameBytes()
        {
            var aligned = await WriteDataFile(nameof(AlignedQueryColumnsWriteTheSameBytes) + "_aligned", Aligned);
            var reordered = await WriteDataFile(nameof(AlignedQueryColumnsWriteTheSameBytes) + "_reordered", Reordered);

            Assert.Equal(aligned, reordered);
        }

        [Fact]
        public async Task AnInsertColumnTheTableLacksFails()
        {
            var testName = nameof(AnInsertColumnTheTableLacksFails);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(Schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            // The declared table has x, the stored table has b
            await stream.StartStream(@"
                CREATE TABLE t (a INT, x STRING, c STRING);
                INSERT INTO t SELECT userKey AS a, lastName AS x, firstName AS c FROM users");

            var error = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "t", stream, 1));

            Assert.Contains("Column x does not exist in the table", error.ToString());
            var files = await storage.Ls(new IOPath("/t/"), recurse: true);
            Assert.Contains(files, x => x.Path.Full == CommitPath("t", 0));
            Assert.DoesNotContain(files, x => x.Path.Full.EndsWith(".parquet", StringComparison.Ordinal));
            Assert.False(await storage.Exists(CommitPath("t", 1)));
        }

        [Fact]
        public async Task AColumnWrittenTwiceFails()
        {
            var testName = nameof(AColumnWrittenTwiceFails);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(Schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            // The planner keeps both columns named a
            await stream.StartStream(@"
                CREATE TABLE t (a INT, b INT, c STRING);
                INSERT INTO t SELECT userKey AS a, userKey + 1000 AS b, firstName AS c, userKey AS a FROM users");

            var error = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "t", stream, 1));

            Assert.Contains("Column a is written more than once", error.ToString());
            Assert.False(await storage.Exists(CommitPath("t", 1)));
        }

        // The data file of the first commit, ten fixed users
        private static async Task<byte[]> WriteDataFile(string testName, string sql)
        {
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(Schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            // Not generated, the generator's seed is shared by every test
            stream.AddUsers(Enumerable.Range(1, 10).Select(x => new User() { UserKey = x, FirstName = $"First{x}" }).ToList());
            await stream.StartStream(sql);
            await WaitForVersion(storage, "t", stream, 1);

            var add = Assert.Single((await ReadCommitActions(storage, "t", 1)).Where(x => x.Add != null)).Add!;
            return await ReadBytes(storage, $"/t/{add.Path}");
        }
    }
}
