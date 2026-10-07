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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeRegressionTests
    {
        private static readonly TimeSpan VersionWaitTimeout = TimeSpan.FromMinutes(2);

        [Theory]
        [InlineData(100)]
        [InlineData(10)]
        public async Task DuplicateRowsAreFullyDeleted(int userCount)
        {
            // 100 users deletes through a deletion vector, 10 users rewrites the file.
            var testName = $"{nameof(DuplicateRowsAreFullyDeleted)}_{userCount}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(testName, storage);

            stream.Generate(userCount);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name FROM users
                UNION ALL
                SELECT userKey, firstName as Name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test", stream, 1);

            Assert.Equal(stream.Users.Count * 2, await LiveRowCount(storage, "test"));

            var expected = stream.Users.SelectMany(x => new[] { new { x.UserKey, x.FirstName }, new { x.UserKey, x.FirstName } });
            await AssertResult(testName, storage, "test", 2, expected);
        }

        [Theory]
        [InlineData(100)]
        [InlineData(10)]
        public async Task DistinctRowsStoredAsTheSameValueAreFullyDeleted(int userCount)
        {
            // Sub millisecond differences are lost on write, both rows store the same timestamp
            var testName = $"{nameof(DistinctRowsStoredAsTheSameValueAreFullyDeleted)}_{userCount}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(testName, storage);

            stream.Generate(userCount);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    ts TIMESTAMP
                );

                INSERT INTO test
                SELECT userKey, timestamp_add('MICROSECOND', 100, birthdate) as ts FROM users
                UNION ALL
                SELECT userKey, timestamp_add('MICROSECOND', 200, birthdate) as ts FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test", stream, 1);

            Assert.Equal(stream.Users.Count * 2, await LiveRowCount(storage, "test"));
        }

        [Fact]
        public async Task DuplicateRowDeletesAreFullyWrittenToChangeData()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(DuplicateRowDeletesAreFullyWrittenToChangeData)}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(DuplicateRowDeletesAreFullyWrittenToChangeData), storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
            });

            stream.Generate(100);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name FROM users
                UNION ALL
                SELECT userKey, firstName as Name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test", stream, 1);

            var commit = await ReadCommitLines(storage, "test", 1);
            var cdcPaths = commit
                .Where(x => x.RootElement.TryGetProperty("cdc", out _))
                .Select(x => x.RootElement.GetProperty("cdc").GetProperty("path").GetString()!)
                .ToList();
            Assert.NotEmpty(cdcPaths);

            // Both copies of the deleted user are change rows
            long deleteRows = 0;
            foreach (var cdcPath in cdcPaths)
            {
                deleteRows += await CountChangeRows(storage, "test", cdcPath, "delete");
            }
            Assert.Equal(2, deleteRows);

            // The source reads the version through the change data files.
            var expected = stream.Users.SelectMany(x => new[] { new { x.UserKey, x.FirstName }, new { x.UserKey, x.FirstName } });
            await AssertResult(nameof(DuplicateRowDeletesAreFullyWrittenToChangeData), storage, "test", 2, expected);
        }

        [Fact]
        public async Task ChangeDataActionsAreNotDataChanges()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ChangeDataActionsAreNotDataChanges)}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(ChangeDataActionsAreNotDataChanges), storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
            });

            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test", stream, 1);

            var commit = await ReadCommitLines(storage, "test", 1);
            var cdcActions = commit
                .Where(x => x.RootElement.TryGetProperty("cdc", out _))
                .Select(x => x.RootElement.GetProperty("cdc"))
                .ToList();

            Assert.NotEmpty(cdcActions);
            Assert.All(cdcActions, cdc => Assert.False(cdc.GetProperty("dataChange").GetBoolean()));
        }

        [Fact]
        public async Task InlineRewriteRollsOutputFiles()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(InlineRewriteRollsOutputFiles)}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(InlineRewriteRollsOutputFiles), storage, options =>
            {
                options.MaxFileSizeBytes = 400;
            });

            stream.Generate(25);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING,
                    LastName STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name, lastName FROM users
            ");

            // Four commits of small files.
            await WaitForVersion(storage, "test", stream, 0);
            for (int version = 1; version < 4; version++)
            {
                stream.Generate(25);
                await WaitForVersion(storage, "test", stream, version);
            }
            var beforeDelete = await DeltaTransactionReader.ReadTable(storage, "test", 3);
            Assert.NotNull(beforeDelete);
            var filesBeforeDelete = beforeDelete.Files.Count;
            Assert.True(filesBeforeDelete >= 4, $"Expected at least one file per commit, got {filesBeforeDelete}");

            // Every fourth row of every file is deleted, chosen per file so every file is rewritten inline
            var keyColumn = beforeDelete.Schema.Fields.First(x => x.Name == "userkey");
            var keyStatsName = keyColumn.PhysicalName ?? keyColumn.Name;
            var toDelete = new List<FlowtideDotNet.AcceptanceTests.Entities.User>();
            foreach (var file in beforeDelete.Files)
            {
                using var stats = JsonDocument.Parse(file.Action.Statistics!);
                var min = stats.RootElement.GetProperty("minValues").GetProperty(keyStatsName).GetInt32();
                var max = stats.RootElement.GetProperty("maxValues").GetProperty(keyStatsName).GetInt32();
                var rows = stream.Users.Where(x => x.UserKey >= min && x.UserKey <= max).OrderBy(x => x.UserKey).ToList();
                Assert.Equal(file.Statistics.NumRecords, rows.Count);
                toDelete.AddRange(rows.Where((_, i) => i % 4 == 0));
            }
            foreach (var user in toDelete)
            {
                stream.DeleteUser(user);
            }

            await WaitForVersion(storage, "test", stream, 4);
            Assert.True(stream.Users.Count == await LiveRowCount(storage, "test"), "The deletes were split across commits, the assertions below need them in one");

            var deleteCommit = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 4);
            Assert.NotNull(deleteCommit);
            Assert.Equal(filesBeforeDelete, deleteCommit.RemovedFiles.Count);
            var rewrittenFiles = deleteCommit.AddedFiles.Where(x => x.DeletionVector == null).ToList();
            var survivors = stream.Users.Count;
            var recordsPerFile = rewrittenFiles.Select(x => JsonDocument.Parse(x.Statistics!).RootElement.GetProperty("numRecords").GetInt32()).ToList();

            Assert.Equal(survivors, recordsPerFile.Sum());
            Assert.True(rewrittenFiles.Count >= 2, $"Expected the rewrite to roll into several files, got {rewrittenFiles.Count} file(s) for {survivors} rows");

            await AssertResult(nameof(InlineRewriteRollsOutputFiles), storage, "test", 5, stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));
        }

        [Fact]
        public async Task ReadDataFileHandlesMissingMiddleColumn()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ReadDataFileHandlesMissingMiddleColumn)}");
            var schema = new StructType(new List<StructField>()
            {
                new StructField("a", new LongType(), true, new Dictionary<string, object>()),
                new StructField("b", new LongType(), true, new Dictionary<string, object>()),
                new StructField("c", new LongType(), true, new Dictionary<string, object>()),
                new StructField("d", new LongType(), true, new Dictionary<string, object>()),
            });

            await WriteParquetFile(storage, "t/part-1.parquet", ("a", 1), ("c", 3), ("d", 4));
            await WriteTable(storage, "t", schema, new List<string>(), "part-1.parquet");

            var table = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string>() { "a", "b", "c", "d" });

            var rows = await ReadSingleRow(reader, storage, "t", "part-1.parquet", null);

            Assert.Equal(new long?[] { 1, null, 3, 4 }, rows);
        }

        [Fact]
        public async Task ReadDataFileHandlesMissingColumnAfterPartitionColumn()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ReadDataFileHandlesMissingColumnAfterPartitionColumn)}");
            var schema = new StructType(new List<StructField>()
            {
                new StructField("p", new LongType(), true, new Dictionary<string, object>()),
                new StructField("a", new LongType(), true, new Dictionary<string, object>()),
                new StructField("b", new LongType(), true, new Dictionary<string, object>()),
                new StructField("c", new LongType(), true, new Dictionary<string, object>()),
            });

            await WriteParquetFile(storage, "t/part-1.parquet", ("a", 1), ("c", 3));
            await WriteTable(storage, "t", schema, new List<string>() { "p" }, "part-1.parquet");

            var table = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string>() { "p", "a", "b", "c" });

            var rows = await ReadSingleRow(reader, storage, "t", "part-1.parquet", new Dictionary<string, string>() { { "p", "7" } });

            Assert.Equal(new long?[] { 7, 1, null, 3 }, rows);
        }

        [Fact]
        public void DateStatisticsResetBetweenFiles()
        {
            var schema = SingleColumnSchema(new DateType());
            var writer = new ParquetSharpWriter(schema, new List<string>() { "v" });

            writer.NewBatch();
            writer.AddRow(SingleValueRow(new TimestampTzValue(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc))));
            writer.AddRow(SingleValueRow(NullValue.Instance));
            writer.NewBatch();
            writer.AddRow(SingleValueRow(new TimestampTzValue(new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc))));

            var stats = SerializeStatistics(schema, writer.GetStatistics());

            Assert.Equal("2024-06-01", stats.GetProperty("minValues").GetProperty("v").GetString());
            Assert.Equal(0, stats.GetProperty("nullCount").GetProperty("v").GetInt32());
        }

        [Fact]
        public async Task MissingStatisticsAreNotInheritedFromPreviousFile()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(MissingStatisticsAreNotInheritedFromPreviousFile)}");
            var schema = new StructType(new List<StructField>()
            {
                new StructField("x", new LongType(), true, new Dictionary<string, object>()),
                new StructField("y", new LongType(), true, new Dictionary<string, object>()),
            });

            await WriteTable(storage, "t", schema, new List<string>(),
                ("first.parquet", "{\"numRecords\":1,\"minValues\":{\"x\":1,\"y\":1},\"maxValues\":{\"x\":10,\"y\":10},\"nullCount\":{\"x\":0,\"y\":0}}"),
                ("second.parquet", "{\"numRecords\":1,\"minValues\":{\"y\":5},\"maxValues\":{\"y\":5},\"nullCount\":{\"y\":0}}"));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.NotNull(table);

            var second = table.Files.Single(x => x.Action.Path == "second.parquet");
            var row = Row(new Int64Value(50), new Int64Value(5));

            // The second file has no bounds for x, so x = 50 cannot be ruled out.
            Assert.True(second.CanBeInFile(row, new List<string>() { "x", "y" }));
        }

        [Fact]
        public void Float32StatisticsDescribeStoredValue()
        {
            var schema = SingleColumnSchema(new FloatType());
            var writer = new ParquetSharpWriter(schema, new List<string>() { "v" });

            writer.NewBatch();
            writer.AddRow(SingleValueRow(new DoubleValue(16777217)));

            var stats = SerializeStatistics(schema, writer.GetStatistics());

            // 16777217 is not representable as a float, the file stores 16777216.
            Assert.Equal(16777216d, stats.GetProperty("minValues").GetProperty("v").GetDouble());
            Assert.Equal(16777216d, stats.GetProperty("maxValues").GetProperty("v").GetDouble());
        }

        [Theory]
        [InlineData(2024, 1, 1, 10, 0, 0, 1234567)]
        [InlineData(1960, 1, 1, 10, 0, 0, 1234567)]
        public void TimestampStatisticsDescribeStoredValue(int year, int month, int day, int hour, int minute, int second, int subSecondTicks)
        {
            var schema = SingleColumnSchema(new TimestampType());
            var writer = new ParquetSharpWriter(schema, new List<string>() { "v" });
            var value = new DateTimeOffset(year, month, day, hour, minute, second, TimeSpan.Zero).AddTicks(subSecondTicks);

            writer.NewBatch();
            writer.AddRow(SingleValueRow(new TimestampTzValue(value)));

            var stored = ((TimestampArray)writer.GetRecordBatch().Column(0)).GetTimestamp(0);
            var stats = SerializeStatistics(schema, writer.GetStatistics());

            Assert.Equal(stored, stats.GetProperty("minValues").GetProperty("v").GetDateTimeOffset());
            Assert.Equal(stored, stats.GetProperty("maxValues").GetProperty("v").GetDateTimeOffset());
        }

        [Theory]
        [InlineData(1e40)]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NegativeInfinity)]
        public void Float32NonFiniteStoredValueKeepsStatisticsSerializable(double value)
        {
            AssertNonFiniteStatistics(new FloatType(), value);
        }

        [Theory]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NegativeInfinity)]
        public void DoubleNonFiniteValueKeepsStatisticsSerializable(double value)
        {
            AssertNonFiniteStatistics(new DoubleType(), value);
        }

        private static void AssertNonFiniteStatistics(SchemaBaseType type, double value)
        {
            var schema = SingleColumnSchema(type);
            var writer = new ParquetSharpWriter(schema, new List<string>() { "v" });

            writer.NewBatch();
            writer.AddRow(SingleValueRow(new DoubleValue(1)));
            writer.AddRow(SingleValueRow(new DoubleValue(value)));

            var statistics = writer.GetStatistics();
            // JSON cannot hold NaN or infinity, the file must still commit
            var stats = SerializeStatistics(schema, statistics);

            Assert.Equal(2, stats.GetProperty("numRecords").GetInt32());
            Assert.False(stats.TryGetProperty("minValues", out var min) && min.TryGetProperty("v", out _));
            Assert.False(stats.TryGetProperty("maxValues", out var max) && max.TryGetProperty("v", out _));
            // Without bounds the file stays a candidate for any probe
            Assert.True(statistics.ValueComparers!["v"].IsInBetween(new DoubleValue(value)));
            Assert.True(statistics.ValueComparers!["v"].IsInBetween(new DoubleValue(1)));
        }

        [Fact]
        public void Float32ProbeMatchesStoredStatistics()
        {
            var schema = SingleColumnSchema(new FloatType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":16777216.0},\"maxValues\":{\"v\":16777216.0},\"nullCount\":{\"v\":0}}");

            // The stream retracts the raw value, the file stores its float rounding.
            Assert.True(stats.ValueComparers!["v"].IsInBetween(new DoubleValue(16777217)));
        }

        [Fact]
        public void Float32ProbeMatchesUnroundedStatistics()
        {
            var schema = SingleColumnSchema(new FloatType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":16777217.0},\"maxValues\":{\"v\":16777217.0},\"nullCount\":{\"v\":0}}");

            // Files written before the fix carry the unrounded value.
            Assert.True(stats.ValueComparers!["v"].IsInBetween(new DoubleValue(16777217)));
        }

        [Fact]
        public void DoubleProbeIsNotRoundedToFloat()
        {
            var schema = SingleColumnSchema(new DoubleType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":16777216.0},\"maxValues\":{\"v\":16777216.0},\"nullCount\":{\"v\":0}}");

            Assert.False(stats.ValueComparers!["v"].IsInBetween(new DoubleValue(16777217)));
        }

        [Fact]
        public void TimestampProbeMatchesStoredStatistics()
        {
            var schema = SingleColumnSchema(new TimestampType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":\"2024-01-01T10:00:00.123+00:00\"},\"maxValues\":{\"v\":\"2024-01-01T10:00:00.123+00:00\"},\"nullCount\":{\"v\":0}}");
            var probe = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero).AddTicks(1234567);

            Assert.True(stats.ValueComparers!["v"].IsInBetween(new TimestampTzValue(probe)));
        }

        [Fact]
        public void TimestampProbeMatchesUntruncatedStatistics()
        {
            var schema = SingleColumnSchema(new TimestampType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":\"2024-01-01T10:00:00.1234567+00:00\"},\"maxValues\":{\"v\":\"2024-01-01T10:00:00.1234567+00:00\"},\"nullCount\":{\"v\":0}}");
            var probe = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero).AddTicks(1234567);

            Assert.True(stats.ValueComparers!["v"].IsInBetween(new TimestampTzValue(probe)));
        }

        [Fact]
        public void DateProbeWithTimeOfDayMatchesDateStatistics()
        {
            var schema = SingleColumnSchema(new DateType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":\"2024-06-01\"},\"maxValues\":{\"v\":\"2024-06-01\"},\"nullCount\":{\"v\":0}}");
            var probe = new TimestampTzValue(new DateTime(2024, 6, 1, 10, 0, 0, DateTimeKind.Utc));

            Assert.True(stats.ValueComparers!["v"].IsInBetween(probe));
        }

        [Fact]
        public void DeletionVectorReAddKeepsTheFileAndMarksADataChange()
        {
            var existing = new DeltaAddAction()
            {
                Path = "part-1.parquet",
                Size = 123,
                ModificationTime = 1000,
                DataChange = false,
                PartitionValues = new Dictionary<string, string>(),
                Statistics = "{\"numRecords\":10,\"minValues\":{\"v\":1},\"maxValues\":{\"v\":10},\"nullCount\":{\"v\":0},\"tightBounds\":true}",
                Tags = new Dictionary<string, string>() { { "k", "v" } },
                BaseRowId = 42,
                DefaultRowCommitVersion = 7,
                ClusteringProvider = "liquid"
            };
            var deletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "abc", Offset = 1, SizeInBytes = 10, Cardinality = 1 };

            var readd = existing.WithDeletionVector(deletionVector);

            Assert.Equal("part-1.parquet", readd.Path);
            Assert.Equal(123, readd.Size);
            Assert.Same(deletionVector, readd.DeletionVector);
            // A delete changes data, even when the file was added by a rearrangement
            Assert.True(readd.DataChange);
            // The physical file is unchanged, so its creation time and identity fields are kept
            Assert.Equal(1000, readd.ModificationTime);
            Assert.Equal(existing.Tags, readd.Tags);
            Assert.Equal(42, readd.BaseRowId);
            Assert.Equal(7, readd.DefaultRowCommitVersion);
            Assert.Equal("liquid", readd.ClusteringProvider);
            // Deleted rows may hold the bounds, so they are no longer tight
            var stats = JsonDocument.Parse(readd.Statistics!).RootElement;
            Assert.False(stats.GetProperty("tightBounds").GetBoolean());
            Assert.Equal(10, stats.GetProperty("numRecords").GetInt32());
            Assert.Equal(1, stats.GetProperty("minValues").GetProperty("v").GetInt32());
        }

        [Fact]
        public async Task DeletionVectorDeleteKeepsFileMetadata()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(DeletionVectorDeleteKeepsFileMetadata)}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(DeletionVectorDeleteKeepsFileMetadata), storage);

            stream.Generate(100);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);
            var initial = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0);
            Assert.NotNull(initial);
            var original = Assert.Single(initial.AddedFiles);

            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test", stream, 1);

            var commit = await ReadCommitLines(storage, "test", 1);
            var readd = commit
                .Where(x => x.RootElement.TryGetProperty("add", out _))
                .Select(x => x.RootElement.GetProperty("add"))
                .Single(x => x.TryGetProperty("deletionVector", out _));

            Assert.Equal(original.Path, readd.GetProperty("path").GetString());
            Assert.True(readd.GetProperty("dataChange").GetBoolean());
            Assert.Equal(original.ModificationTime, readd.GetProperty("modificationTime").GetInt64());
            using var stats = JsonDocument.Parse(readd.GetProperty("stats").GetString()!);
            Assert.False(stats.RootElement.GetProperty("tightBounds").GetBoolean());
        }

        [Fact]
        public void MissingNullCountIsUnknown()
        {
            var schema = SingleColumnSchema(new LongType());
            var stats = DeserializeStatistics(schema, "{\"numRecords\":1,\"minValues\":{\"v\":1},\"maxValues\":{\"v\":10}}");

            // Without a null count the file may hold nulls
            Assert.True(stats.ValueComparers!["v"].IsInBetween(NullValue.Instance));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DeleteMatchingUsesEngineFloatEquality(bool isFloat32)
        {
            SchemaBaseType type = isFloat32 ? new FloatType() : new DoubleType();
            var comparer = RecordBatchComparer.Create(SingleColumnSchema(type));

            using var toFind = FloatBatch(isFloat32, double.NaN, -0.0);
            using var searchIn = FloatBatch(isFloat32, 1.0, 0.0, double.NaN);

            // The engine merges NaN with NaN and -0 with +0, deletes must match them
            Assert.Equal(2, comparer.FindOccurance(0, toFind, searchIn, 0, EmptyDeleteVector.Instance));
            Assert.Equal(1, comparer.FindOccurance(1, toFind, searchIn, 0, EmptyDeleteVector.Instance));
        }

        [Fact]
        public async Task CopyFromRollsBetweenBatchesAndCountsSurvivors()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CopyFromRollsBetweenBatchesAndCountsSurvivors)}");
            var schema = SingleColumnSchema(new LongType());
            var names = new List<string>() { "v" };

            var sources = new List<string>();
            var source = new ParquetSharpWriter(schema, names);
            for (int file = 0; file < 4; file++)
            {
                source.NewBatch();
                for (int i = 0; i < 25; i++)
                {
                    source.AddRow(SingleValueRow(new Int64Value(file * 100 + i)));
                }
                var path = $"source-{file}.parquet";
                await source.WriteData(storage, "t", path);
                sources.Add(path);
            }

            var writer = new ParquetSharpWriter(schema, names);
            writer.NewBatch();
            var rolledCounts = new List<int>();
            foreach (var path in sources)
            {
                var deleted = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
                for (int i = 0; i < 25; i += 4)
                {
                    deleted.Add(i);
                }
                await writer.CopyFrom(storage, "t", path, deleted, w => w.WrittenBytes >= 100, () =>
                {
                    rolledCounts.Add(writer.WrittenCount);
                    writer.NewBatch();
                    return Task.CompletedTask;
                });
            }

            // 7 of 25 rows are deleted per file, every copied file fills the writer
            Assert.Equal(new List<int>() { 18, 18, 18, 18 }, rolledCounts);
            Assert.Equal(0, writer.WrittenCount);
        }

        [Fact]
        public async Task MultiplicityExpansionRollsFiles()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(MultiplicityExpansionRollsFiles)}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(MultiplicityExpansionRollsFiles), storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Every row fills a file
                options.MaxFileSizeBytes = 1;
            });

            stream.Generate(5);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name FROM users
                UNION ALL
                SELECT userKey, firstName as Name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            // Each copy of a weight 2 row rolls its own file
            var initial = await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0);
            Assert.NotNull(initial);
            Assert.Equal(10, initial.AddedFiles.Count);
            Assert.All(initial.AddedFiles, add => Assert.Equal(1, NumRecords(add)));

            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test", stream, 1);

            var commit = await ReadCommitLines(storage, "test", 1);
            var cdcPaths = commit
                .Where(x => x.RootElement.TryGetProperty("cdc", out _))
                .Select(x => x.RootElement.GetProperty("cdc").GetProperty("path").GetString()!)
                .ToList();
            Assert.Equal(2, cdcPaths.Count);
            foreach (var cdcPath in cdcPaths)
            {
                Assert.Equal(1, await CountChangeRows(storage, "test", cdcPath, "delete"));
            }

            // An update keeps its change files, so inserted copies must roll too
            var updated = stream.Users[0];
            updated.FirstName = "Renamed";
            stream.AddOrUpdateUser(updated);

            await WaitForVersion(storage, "test", stream, 2);

            var updateCommit = await ReadCommitLines(storage, "test", 2);
            var updateCdcPaths = updateCommit
                .Where(x => x.RootElement.TryGetProperty("cdc", out _))
                .Select(x => x.RootElement.GetProperty("cdc").GetProperty("path").GetString()!)
                .ToList();
            Assert.Equal(4, updateCdcPaths.Count);
            long deletes = 0;
            long inserts = 0;
            foreach (var cdcPath in updateCdcPaths)
            {
                var fileDeletes = await CountChangeRows(storage, "test", cdcPath, "delete");
                var fileInserts = await CountChangeRows(storage, "test", cdcPath, "insert");
                Assert.Equal(1, fileDeletes + fileInserts);
                deletes += fileDeletes;
                inserts += fileInserts;
            }
            Assert.Equal(2, deletes);
            Assert.Equal(2, inserts);

            var expected = stream.Users.SelectMany(x => new[] { new { x.UserKey, x.FirstName }, new { x.UserKey, x.FirstName } });
            await AssertResult(nameof(MultiplicityExpansionRollsFiles), storage, "test", 3, expected);
        }

        [Fact]
        public async Task CopyFromCountsSurvivorsAcrossBatches()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CopyFromCountsSurvivorsAcrossBatches)}");
            var schema = SingleColumnSchema(new LongType());
            var names = new List<string>() { "v" };
            const int rows = 150_000;

            var column = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < rows; i++)
            {
                column.Add(new Int64Value(i));
            }
            using var data = new EventBatchData(new IColumn[] { column });
            var source = new ParquetSharpWriter(schema, names);
            source.NewBatch();
            for (int i = 0; i < rows; i++)
            {
                source.AddRow(new ColumnRowReference() { referenceBatch = data, RowIndex = i });
            }
            await source.WriteData(storage, "t", "source.parquet");

            var batchLengths = await ReadBatchLengths(storage, "t/source.parquet");
            Assert.True(batchLengths.Count > 1, "The source file must be read in several batches");

            // Every third row plus both edges of each batch
            var positions = new SortedSet<long>();
            for (long i = 0; i < rows; i += 3)
            {
                positions.Add(i);
            }
            long start = 0;
            foreach (var length in batchLengths)
            {
                positions.Add(start);
                positions.Add(start + length - 1);
                start += length;
            }
            var deleted = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
            foreach (var position in positions)
            {
                deleted.Add(position);
            }

            var writer = new ParquetSharpWriter(schema, names);
            writer.NewBatch();
            var rolledCounts = new List<int>();
            await writer.CopyFrom(storage, "t", "source.parquet", deleted, _ => true, () =>
            {
                rolledCounts.Add(writer.WrittenCount);
                writer.NewBatch();
                return Task.CompletedTask;
            });

            // A roll after every batch exposes the per batch count
            var expected = new List<int>();
            long offset = 0;
            foreach (var length in batchLengths)
            {
                expected.Add(length - positions.GetViewBetween(offset, offset + length - 1).Count);
                offset += length;
            }
            Assert.Equal(expected, rolledCounts);
        }

        [Fact]
        public void Float32CopyArrayReportsCopiedBytes()
        {
            var writer = new ParquetFloat32Writer();
            writer.NewBatch();

            var builder = new FloatArray.Builder();
            for (int i = 0; i < 10; i++)
            {
                builder.Append(i);
            }
            using var array = builder.Build();

            var bytes = writer.CopyArray(array, 0, EmptyDeleteVector.Instance, 0, array.Length);

            Assert.Equal(40, bytes);
        }

        [Theory]
        [InlineData("Tami")]
        [InlineData("Tom")]
        public async Task BinaryRowSharingFirstByteWithMaxIsDeleted(string deletedName)
        {
            // Tami and Tom share their first byte with the max, Tom
            var testName = $"{nameof(BinaryRowSharingFirstByteWithMaxIsDeleted)}_{deletedName}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(testName, storage);

            AddUsers(stream, "Aaron", "Bob", "Tami", "Tom");

            await stream.StartStream(@"
                CREATE TABLE test (
                    firstName BINARY
                );

                INSERT INTO test
                SELECT firstName FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            stream.DeleteUser(stream.Users.Single(x => x.FirstName == deletedName));

            await WaitForVersion(storage, "test", stream, 1);

            await AssertResult(testName, storage, "test", 2, stream.Users.Select(x => new { val = Encoding.UTF8.GetBytes(x.FirstName!) }));
        }

        [Fact]
        public async Task NonUtf8BinaryRowsAreDeleted()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(NonUtf8BinaryRowsAreDeleted)}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(NonUtf8BinaryRowsAreDeleted), storage);

            AddUsers(stream, "a", "b", "c", "d");

            await stream.StartStream(@"
                CREATE TABLE test (
                    v BINARY
                );

                INSERT INTO test
                SELECT b.v
                FROM users u
                INNER JOIN (
                    VALUES (1, 0x41), (2, 0xC3), (3, 0x41FF42), (4, 0xFF)
                ) b(k, v) ON u.userKey = b.k
            ");

            await WaitForVersion(storage, "test", stream, 0);

            // 0xFF is the max of the file
            stream.DeleteUser(stream.Users.Single(x => x.UserKey == 4));

            await WaitForVersion(storage, "test", stream, 1);

            // 0x41 is the min of the rewritten file
            stream.DeleteUser(stream.Users.Single(x => x.UserKey == 1));

            await WaitForVersion(storage, "test", stream, 2);

            await AssertResult(nameof(NonUtf8BinaryRowsAreDeleted), storage, "test", 3, new[]
            {
                new { v = new byte[] { 0xC3 } },
                new { v = new byte[] { 0x41, 0xFF, 0x42 } }
            });
        }

        [Fact]
        public void BinaryStatisticsRoundTripAsUtf8()
        {
            var schema = SingleColumnSchema(new BinaryType());
            var writer = new ParquetSharpWriter(schema, new List<string>() { "v" });

            writer.NewBatch();
            writer.AddRow(SingleValueRow(new BinaryValue(Encoding.UTF8.GetBytes("Tom"))));
            writer.AddRow(SingleValueRow(new BinaryValue(Encoding.UTF8.GetBytes("Aaron"))));
            writer.AddRow(SingleValueRow(NullValue.Instance));

            var json = SerializeStatistics(schema, writer.GetStatistics());

            Assert.Equal("Aaron", json.GetProperty("minValues").GetProperty("v").GetString());
            Assert.Equal("Tom", json.GetProperty("maxValues").GetProperty("v").GetString());
            Assert.Equal(1, json.GetProperty("nullCount").GetProperty("v").GetInt32());

            var comparer = DeserializeStatistics(schema, json.GetRawText()).ValueComparers!["v"];

            Assert.True(comparer.IsInBetween(new BinaryValue(Encoding.UTF8.GetBytes("Tami"))));
            Assert.True(comparer.IsInBetween(new BinaryValue(Encoding.UTF8.GetBytes("Tom"))));
            // Bounds still prune
            Assert.False(comparer.IsInBetween(new BinaryValue(Encoding.UTF8.GetBytes("Zed"))));
        }

        [Fact]
        public void BinaryStatisticsSkipInvalidUtf8Bounds()
        {
            var schema = SingleColumnSchema(new BinaryType());
            var writer = new ParquetSharpWriter(schema, new List<string>() { "v" });

            writer.NewBatch();
            writer.AddRow(SingleValueRow(new BinaryValue(new byte[] { 0xFF })));
            writer.AddRow(SingleValueRow(new BinaryValue(new byte[] { 0x41 })));

            var json = SerializeStatistics(schema, writer.GetStatistics());

            // 0xFF has no json text form
            Assert.Equal("A", json.GetProperty("minValues").GetProperty("v").GetString());
            Assert.False(json.GetProperty("maxValues").TryGetProperty("v", out _));
        }

        [Theory]
        // Older Flowtide, bytes written as utf8 text
        [InlineData("Aaron", "Tom", "54616D69")]
        [InlineData("Aaron", "Tom", "546F6D")]
        [InlineData("A", "Tom", "41")]
        // Older Flowtide, invalid utf8 replaced on write
        [InlineData("A", "\uFFFD", "FF")]
        [InlineData("\uFFFD", "\uFFFD", "C3")]
        public void LegacyBinaryBoundsKeepTheirRows(string min, string max, string probeHex)
        {
            var schema = SingleColumnSchema(new BinaryType());
            var stats = DeserializeStatistics(schema, JsonSerializer.Serialize(new
            {
                numRecords = 2,
                minValues = new { v = min },
                maxValues = new { v = max },
                nullCount = new { v = 0 }
            }));

            Assert.True(stats.ValueComparers!["v"].IsInBetween(new BinaryValue(Convert.FromHexString(probeHex))));
            // The null count still rules out nulls
            Assert.False(stats.ValueComparers!["v"].IsInBetween(NullValue.Instance));
        }

        [Theory]
        [InlineData("BINARY", true, 20)]
        [InlineData("BINARY", false, 3)]
        [InlineData("STRING", true, 20)]
        [InlineData("STRING", false, 3)]
        public async Task EmptyValueDeleteKeepsNullRow(string type, bool deletionVectors, int rowCount)
        {
            // 20 rows deletes through a deletion vector, 3 rows rewrites the file
            var testName = $"{nameof(EmptyValueDeleteKeepsNullRow)}_{type}_{deletionVectors}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(testName, storage, o => o.EnableDeletionVectorsOnNewTables = deletionVectors);

            var names = new List<string?>() { null, "" };
            for (int i = 2; i < rowCount; i++)
            {
                names.Add($"n{i:D2}");
            }
            AddUsers(stream, names.ToArray());

            await stream.StartStream($@"
                CREATE TABLE test (
                    firstName {type}
                );

                INSERT INTO test
                SELECT firstName FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            // Nulls sort first, the empty delete matched the null row
            stream.DeleteUser(stream.Users.Single(x => x.FirstName == ""));
            await WaitForVersion(storage, "test", stream, 1);
            await AssertUsers($"{testName}_empty", 2);

            // Rewrite without the null row made this delete throw
            stream.DeleteUser(stream.Users.Single(x => x.FirstName == null));
            await WaitForVersion(storage, "test", stream, 2);
            await AssertUsers($"{testName}_null", 3);

            Task AssertUsers(string name, int waitCount)
            {
                if (type == "BINARY")
                {
                    return AssertResult(name, storage, "test", waitCount, stream.Users.Select(x => new { val = x.FirstName == null ? null : Encoding.UTF8.GetBytes(x.FirstName) }));
                }
                return AssertResult(name, storage, "test", waitCount, stream.Users.Select(x => new { val = x.FirstName }));
            }
        }

        [Theory]
        [InlineData("BINARY")]
        [InlineData("STRING")]
        public async Task EmptyValueDeleteKeepsNullRowInSecondColumn(string type)
        {
            // Columns after the first are matched by IsEqual
            var testName = $"{nameof(EmptyValueDeleteKeepsNullRowInSecondColumn)}_{type}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(testName, storage);

            AddUsers(stream, null, "", "n02");

            await stream.StartStream($@"
                CREATE TABLE test (
                    lastName STRING,
                    firstName {type}
                );

                INSERT INTO test
                SELECT lastName, firstName FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            stream.DeleteUser(stream.Users.Single(x => x.FirstName == ""));
            await WaitForVersion(storage, "test", stream, 1);

            if (type == "BINARY")
            {
                await AssertResult(testName, storage, "test", 2, stream.Users.Select(x => new { lastName = x.LastName, firstName = x.FirstName == null ? null : Encoding.UTF8.GetBytes(x.FirstName) }));
            }
            else
            {
                await AssertResult(testName, storage, "test", 2, stream.Users.Select(x => new { lastName = x.LastName, firstName = x.FirstName }));
            }
        }

        private static void AddUsers(FlowtideTestStream stream, params string?[] firstNames)
        {
            for (int i = 0; i < firstNames.Length; i++)
            {
                stream.AddOrUpdateUser(new FlowtideDotNet.AcceptanceTests.Entities.User()
                {
                    UserKey = i + 1,
                    FirstName = firstNames[i],
                    LastName = "Last"
                });
            }
        }

        private static StructType SingleColumnSchema(SchemaBaseType type)
        {
            return new StructType(new List<StructField>()
            {
                new StructField("v", type, true, new Dictionary<string, object>())
            });
        }

        private static ColumnRowReference SingleValueRow<T>(T value) where T : IDataValue
        {
            return Row(value);
        }

        private static ColumnRowReference Row(params IDataValue[] values)
        {
            var columns = new IColumn[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                var column = FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
                column.Add(values[i]);
                columns[i] = column;
            }
            return new ColumnRowReference() { referenceBatch = new EventBatchData(columns), RowIndex = 0 };
        }

        private static JsonElement SerializeStatistics(StructType schema, DeltaStatistics statistics)
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new DeltaStatisticsConverter(schema));
            var json = JsonSerializer.Serialize(statistics, options);
            return JsonDocument.Parse(json).RootElement;
        }

        private static DeltaStatistics DeserializeStatistics(StructType schema, string json)
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new DeltaStatisticsConverter(schema));
            return JsonSerializer.Deserialize<DeltaStatistics>(json, options)!;
        }

        private static async Task WriteParquetFile(IFileStorage storage, string path, params (string Name, long Value)[] columns)
        {
            var fields = columns.Select(x => new Field(x.Name, Apache.Arrow.Types.Int64Type.Default, true)).ToList();
            var arrowSchema = new Apache.Arrow.Schema(fields, null);
            var arrays = columns.Select(x => (IArrowArray)new Int64Array.Builder().Append(x.Value).Build()).ToArray();
            using var batch = new RecordBatch(arrowSchema, arrays, 1);

            using var memory = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(memory, arrowSchema))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }

            using var target = await storage.OpenWrite(path);
            Assert.NotNull(target);
            await target.WriteAsync(memory.ToArray());
        }

        private static Task WriteTable(IFileStorage storage, string tableName, StructType schema, List<string> partitionColumns, string dataFile)
        {
            return WriteTable(storage, tableName, schema, partitionColumns, (dataFile, null));
        }

        private static async Task WriteTable(IFileStorage storage, string tableName, StructType schema, List<string> partitionColumns, params (string Path, string? Stats)[] files)
        {
            var jsonOptions = new JsonSerializerOptions();
            jsonOptions.Converters.Add(new TypeConverter());

            var actions = new List<DeltaAction>()
            {
                new DeltaAction()
                {
                    Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 }
                },
                new DeltaAction()
                {
                    MetaData = new DeltaMetadataAction()
                    {
                        Id = Guid.NewGuid().ToString(),
                        SchemaString = JsonSerializer.Serialize(schema as SchemaBaseType, jsonOptions),
                        PartitionColumns = partitionColumns,
                        Format = new DeltaMetadataFormat() { Provider = "parquet", Options = new Dictionary<string, string>() },
                        Configuration = new Dictionary<string, string>()
                    }
                }
            };

            foreach (var file in files)
            {
                actions.Add(new DeltaAction()
                {
                    Add = new DeltaAddAction()
                    {
                        Path = file.Path,
                        Size = 1,
                        DataChange = true,
                        PartitionValues = new Dictionary<string, string>(),
                        Statistics = file.Stats
                    }
                });
            }

            await DeltaTransactionWriter.WriteCommit(storage, tableName, 0, actions);
        }

        private static async Task<long?[]> ReadSingleRow(ParquetSharpReader reader, IFileStorage storage, string table, string path, Dictionary<string, string>? partitionValues)
        {
            await foreach (var batch in reader.ReadDataFile(storage, table, path, EmptyDeleteVector.Instance, partitionValues, GlobalMemoryManager.Instance))
            {
                Assert.Equal(1, batch.count);
                var result = new long?[batch.data.Columns.Count];
                for (int i = 0; i < result.Length; i++)
                {
                    var value = batch.data.Columns[i].GetValueAt(0, default);
                    result[i] = value.IsNull ? null : value.AsLong;
                }
                batch.data.Dispose();
                return result;
            }
            throw new InvalidOperationException("The file produced no rows");
        }

        private static RecordBatch FloatBatch(bool isFloat32, params double[] values)
        {
            IArrowArray array;
            Field field;
            if (isFloat32)
            {
                var builder = new FloatArray.Builder();
                foreach (var value in values)
                {
                    builder.Append((float)value);
                }
                array = builder.Build();
                field = new Field("v", Apache.Arrow.Types.FloatType.Default, true);
            }
            else
            {
                var builder = new DoubleArray.Builder();
                foreach (var value in values)
                {
                    builder.Append(value);
                }
                array = builder.Build();
                field = new Field("v", Apache.Arrow.Types.DoubleType.Default, true);
            }
            return new RecordBatch(new Apache.Arrow.Schema(new List<Field>() { field }, null), new[] { array }, values.Length);
        }

        private static int NumRecords(DeltaAddAction add)
        {
            using var stats = JsonDocument.Parse(add.Statistics!);
            return stats.RootElement.GetProperty("numRecords").GetInt32();
        }

        private static async Task<List<int>> ReadBatchLengths(IFileStorage storage, string path)
        {
            using var stream = await storage.OpenRead(path);
            Assert.NotNull(stream);
            using var memory = new MemoryStream();
            await stream.CopyToAsync(memory);
            memory.Position = 0;
            using var reader = new ParquetSharp.Arrow.FileReader(memory);
            using var batches = reader.GetRecordBatchReader();
            var lengths = new List<int>();
            RecordBatch batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    lengths.Add(batch.Length);
                }
            }
            return lengths;
        }

        private static async Task<long> CountChangeRows(IFileStorage storage, string tableName, string path, string changeType)
        {
            using var stream = await storage.OpenRead($"/{tableName}/{path}");
            Assert.NotNull(stream);
            using var memory = new MemoryStream();
            await stream.CopyToAsync(memory);
            memory.Position = 0;
            using var reader = new ParquetSharp.Arrow.FileReader(memory);
            using var batches = reader.GetRecordBatchReader();
            long count = 0;
            RecordBatch batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    var changes = (StringArray)batch.Column("_change_type");
                    for (int i = 0; i < changes.Length; i++)
                    {
                        if (changes.GetString(i) == changeType)
                        {
                            count++;
                        }
                    }
                }
            }
            return count;
        }

        private static async Task<long> LiveRowCount(IFileStorage storage, string tableName)
        {
            var table = await DeltaTransactionReader.ReadTable(storage, tableName);
            Assert.NotNull(table);
            return table.Files.Sum(x => x.Statistics.NumRecords - (x.Action.DeletionVector?.Cardinality ?? 0));
        }

        private static async Task<List<JsonDocument>> ReadCommitLines(IFileStorage storage, string tableName, long version)
        {
            using var stream = await storage.OpenRead($"/{tableName}/_delta_log/{version:D20}.json");
            Assert.NotNull(stream);
            using var reader = new StreamReader(stream);
            var lines = new List<JsonDocument>();
            string? line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (line.Length > 0)
                {
                    lines.Add(JsonDocument.Parse(line));
                }
            }
            return lines;
        }

        private static async Task AssertResult<T>(string testName, IFileStorage storage, string tableName, int waitCount, IEnumerable<T> result)
        {
            DeltaLakeTestStream stream = new DeltaLakeTestStream(testName + "_compare", storage);

            await stream.StartStream($"INSERT INTO result SELECT * FROM {tableName}");

            for (int i = 0; i < waitCount; i++)
            {
                await stream.WaitForUpdate();
            }

            stream.AssertCurrentDataEqual(result);
            await stream.DisposeAsync();
        }

        private static async Task WaitForVersion(IFileStorage storage, string tableName, FlowtideTestStream stream, long version)
        {
            var deadline = DateTime.UtcNow + VersionWaitTimeout;
            while (!await storage.Exists($"/{tableName}/_delta_log/{version:D20}.json"))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Version {version} of {tableName} was not written within {VersionWaitTimeout}");
                }
                await stream.SchedulerTick();
                await Task.Delay(100);
            }
        }
    }
}
