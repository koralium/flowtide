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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLogReplayTests
    {
        private const string GoldenTable = "table_with_deletion_logs";
        private const string IdSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
        private const string UserSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"userkey\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
        private static readonly TimeSpan VersionWaitTimeout = TimeSpan.FromMinutes(2);

        [Fact]
        public async Task NewestCheckpointMatchesFullReplay()
        {
            var withCheckpoints = await CopyGoldenLog($"{nameof(NewestCheckpointMatchesFullReplay)}_all", _ => true);
            var jsonOnly = await CopyGoldenLog($"{nameof(NewestCheckpointMatchesFullReplay)}_json", IsNotCheckpoint);

            var table = await DeltaTransactionReader.ReadTable(withCheckpoints, GoldenTable);
            var replay = await DeltaTransactionReader.ReadTable(jsonOnly, GoldenTable);

            Assert.NotNull(table);
            Assert.NotNull(replay);
            Assert.Equal(20, table.StartCheckpointVersion);
            Assert.Null(replay.StartCheckpointVersion);
            Assert.Equal(20, table.Version);
            Assert.NotEmpty(replay.Tombstones);
            Assert.Equal(Describe(replay), Describe(table));
        }

        [Fact]
        public async Task TimeTravelStartsFromTheNewestCheckpointAtOrBelowTheVersion()
        {
            var withCheckpoints = await CopyGoldenLog($"{nameof(TimeTravelStartsFromTheNewestCheckpointAtOrBelowTheVersion)}_all", _ => true);
            var jsonOnly = await CopyGoldenLog($"{nameof(TimeTravelStartsFromTheNewestCheckpointAtOrBelowTheVersion)}_json", IsNotCheckpoint);

            var table = await DeltaTransactionReader.ReadTable(withCheckpoints, GoldenTable, 15);
            var replay = await DeltaTransactionReader.ReadTable(jsonOnly, GoldenTable, 15);

            Assert.NotNull(table);
            Assert.NotNull(replay);
            Assert.Equal(10, table.StartCheckpointVersion);
            Assert.Equal(15, table.Version);
            Assert.Equal(Describe(replay), Describe(table));
        }

        [Fact]
        public async Task VersionZeroIsTheFirstCommitWhenItExists()
        {
            var storage = await CopyGoldenLog(nameof(VersionZeroIsTheFirstCommitWhenItExists), _ => true);

            var table = await DeltaTransactionReader.ReadTable(storage, GoldenTable, 0);

            Assert.NotNull(table);
            Assert.Equal(0, table.Version);
            Assert.Null(table.StartCheckpointVersion);
        }

        [Fact]
        public async Task CleanedLogStartsFromTheRetainedCheckpoint()
        {
            // Metadata cleanup removed everything below checkpoint 20
            var cleaned = await CopyGoldenLog($"{nameof(CleanedLogStartsFromTheRetainedCheckpoint)}_cleaned", name => name.StartsWith("00000000000000000020."));
            var jsonOnly = await CopyGoldenLog($"{nameof(CleanedLogStartsFromTheRetainedCheckpoint)}_json", IsNotCheckpoint);

            var table = await DeltaTransactionReader.ReadTable(cleaned, GoldenTable);
            var replay = await DeltaTransactionReader.ReadTable(jsonOnly, GoldenTable);
            var oldest = await DeltaTransactionReader.ReadTable(cleaned, GoldenTable, 0);

            Assert.NotNull(table);
            Assert.NotNull(replay);
            Assert.NotNull(oldest);
            Assert.Equal(20, table.StartCheckpointVersion);
            Assert.Equal(Describe(replay), Describe(table));
            Assert.Equal(20, oldest.Version);
            await Assert.ThrowsAsync<DeltaVersionNotAvailableException>(() => DeltaTransactionReader.ReadTable(cleaned, GoldenTable, 15));
        }

        [Fact]
        public async Task TornNewestCheckpointFallsBackToTheOlderOne()
        {
            var storage = await CopyGoldenLog($"{nameof(TornNewestCheckpointFallsBackToTheOlderOne)}_all", _ => true);
            var jsonOnly = await CopyGoldenLog($"{nameof(TornNewestCheckpointFallsBackToTheOlderOne)}_json", IsNotCheckpoint);
            await Truncate(storage, $"/{GoldenTable}/_delta_log/00000000000000000020.checkpoint.parquet");

            var table = await DeltaTransactionReader.ReadTable(storage, GoldenTable);
            var replay = await DeltaTransactionReader.ReadTable(jsonOnly, GoldenTable);

            Assert.NotNull(table);
            Assert.NotNull(replay);
            Assert.Equal(10, table.StartCheckpointVersion);
            Assert.Equal(Describe(replay), Describe(table));
        }

        [Fact]
        public async Task UnreadableCheckpointsFallBackToFullReplay()
        {
            var storage = await CopyGoldenLog($"{nameof(UnreadableCheckpointsFallBackToFullReplay)}_all", _ => true);
            var jsonOnly = await CopyGoldenLog($"{nameof(UnreadableCheckpointsFallBackToFullReplay)}_json", IsNotCheckpoint);
            await Truncate(storage, $"/{GoldenTable}/_delta_log/00000000000000000020.checkpoint.parquet");
            await Truncate(storage, $"/{GoldenTable}/_delta_log/00000000000000000010.checkpoint.parquet");

            var table = await DeltaTransactionReader.ReadTable(storage, GoldenTable);
            var replay = await DeltaTransactionReader.ReadTable(jsonOnly, GoldenTable);

            Assert.NotNull(table);
            Assert.NotNull(replay);
            Assert.Null(table.StartCheckpointVersion);
            Assert.Equal(Describe(replay), Describe(table));
        }

        [Fact]
        public async Task CheckpointWithSidecarsIsNotUsed()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointWithSidecarsIsNotUsed)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            // A V2 checkpoint keeps its file actions in sidecars, reading it alone would lose them.
            // The sidecar column comes last, so the ghost add is already applied when the read fails.
            await WriteClassicCheckpoint(storage, "t", 1,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "ghost.parquet", 1L, 0L, true, null }),
                ("sidecar", new object?[] { "016ae953-37a9-438e-8683-9a9a4a79a395.parquet", 1L, 0L }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Null(table.StartCheckpointVersion);
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table.AddFiles.Select(x => x.Path).Order());
        }

        [Fact]
        public async Task CheckpointWithAnUnreadableSchemaIsNotUsed()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointWithAnUnreadableSchemaIsNotUsed)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            await WriteClassicCheckpoint(storage, "t", 1,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", "not a schema", 0L }),
                ("add", new object?[] { "ghost.parquet", 1L, 0L, true, null }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Null(table.StartCheckpointVersion);
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table.AddFiles.Select(x => x.Path).Order());
        }

        [Fact]
        public async Task CheckpointSchemaReplacedByALaterCommitIsNotAProblem()
        {
            // The checkpoint is the only start, and the commit after it replaces the schema the reader cannot parse
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointSchemaReplacedByALaterCommitIsNotAProblem)}");
            await WriteClassicCheckpoint(storage, "t", 1,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", "not a schema", 0L }),
                ("add", new object?[] { "a.parquet", 1L, 0L, true, null }));
            await WriteCommit(storage, "t", 2, Metadata(IdSchema), Add("b.parquet"));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Equal(1, table.StartCheckpointVersion);
            Assert.Equal(IdSchema, table.Metadata.SchemaString);
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table.AddFiles.Select(x => x.Path).Order());
        }

        [Fact]
        public async Task OldestVersionSkipsACheckpointWithAnUnreadableSchema()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(OldestVersionSkipsACheckpointWithAnUnreadableSchema)}");
            await WriteClassicCheckpoint(storage, "t", 1,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", "not a schema", 0L }),
                ("add", new object?[] { "ghost.parquet", 1L, 0L, true, null }));
            await WriteClassicCheckpoint(storage, "t", 2,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "a.parquet", 1L, 0L, true, null }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t", 0);

            Assert.NotNull(table);
            Assert.Equal(2, table.Version);
            Assert.Equal(new[] { "a.parquet" }, table.AddFiles.Select(x => x.Path));
        }

        [Fact]
        public async Task CheckpointWithAnUnsupportedColumnTypeIsNotUsed()
        {
            // A visitor without a case for the type used to recurse until the stack overflowed
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointWithAnUnsupportedColumnTypeIsNotUsed)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            var columns = new Dictionary<string, (string Name, IArrowType Type)[]>(CheckpointColumns)
            {
                ["add"] = new (string, IArrowType)[] { ("path", StringType.Default), ("size", DoubleType.Default), ("modificationTime", Int64Type.Default), ("dataChange", BooleanType.Default), ("stats", StringType.Default) }
            };
            await WriteClassicCheckpoint(storage, "t", 1, columns,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "ghost.parquet", 1.5d, 0L, true, null }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Null(table.StartCheckpointVersion);
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table.AddFiles.Select(x => x.Path).Order());
        }

        [Fact]
        public async Task NullFeatureListsStayNullThroughCheckpoints()
        {
            // Feature lists are only allowed on reader version 3 and writer version 7
            var storage = Files.Of.InternalMemory($"./{nameof(NullFeatureListsStayNullThroughCheckpoints)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            var fromCommits = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.NotNull(fromCommits);
            await DeltaCheckpointWriter.WriteCheckpoint(storage, "t", fromCommits);

            var fromCheckpoint = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.NotNull(fromCheckpoint);
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            var next = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.NotNull(next);
            await DeltaCheckpointWriter.WriteCheckpoint(storage, "t", next);
            var fromSecondCheckpoint = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.Equal(0, fromCheckpoint.StartCheckpointVersion);
            Assert.Null(fromCheckpoint.Protocol.ReaderFeatures);
            Assert.Null(fromCheckpoint.Protocol.WriterFeatures);
            Assert.NotNull(fromSecondCheckpoint);
            Assert.Equal(1, fromSecondCheckpoint.StartCheckpointVersion);
            Assert.Null(fromSecondCheckpoint.Protocol.ReaderFeatures);
            Assert.Null(fromSecondCheckpoint.Protocol.WriterFeatures);
        }

        [Fact]
        public async Task AddClearsAnEarlierTombstone()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(AddClearsAnEarlierTombstone)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Remove("a.parquet"));
            await WriteCommit(storage, "t", 2, Add("a.parquet"));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Equal(new[] { "a.parquet" }, table.AddFiles.Select(x => x.Path));
            Assert.Empty(table.Tombstones);
        }

        [Fact]
        public async Task AddAfterACheckpointClearsItsTombstone()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(AddAfterACheckpointClearsItsTombstone)}");
            await WriteClassicCheckpoint(storage, "t", 1,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("remove", new object?[] { "r.parquet", 5L, true, 1L }));
            await WriteCommit(storage, "t", 2, Add("r.parquet"));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Equal(1, table.StartCheckpointVersion);
            Assert.Equal(new[] { "r.parquet" }, table.AddFiles.Select(x => x.Path));
            Assert.Empty(table.Tombstones);
        }

        [Fact]
        public async Task CheckpointReadCanLeaveOutRemoves()
        {
            var storage = Files.Of.LocalDisk("../../../testdata");
            var checkpoint = new IOEntry($"/{GoldenTable}/_delta_log/00000000000000000020.checkpoint.parquet");
            var reader = new FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading.ParquetCheckpointReader();

            var withRemoves = new List<DeltaAction>();
            await foreach (var action in reader.ReadCheckpointFile(storage, checkpoint))
            {
                withRemoves.Add(action);
            }
            var withoutRemoves = new List<DeltaAction>();
            await foreach (var action in reader.ReadCheckpointFile(storage, checkpoint, includeRemoves: false))
            {
                withoutRemoves.Add(action);
            }

            Assert.Contains(withRemoves, x => x.Remove != null);
            Assert.DoesNotContain(withoutRemoves, x => x.Remove != null);
            Assert.Equal(withRemoves.Count(x => x.Add != null), withoutRemoves.Count(x => x.Add != null));
        }

        [Fact]
        public async Task CheckpointWithCommitInfoAndCheckpointMetadataRowsIsUsed()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointWithCommitInfoAndCheckpointMetadataRowsIsUsed)}");
            await WriteClassicCheckpoint(storage, "t", 1,
                ("checkpointMetadata", new object?[] { 1L }),
                ("commitInfo", new object?[] { "WRITE" }),
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "a.parquet", 1L, 0L, true, null }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Equal(1, table.StartCheckpointVersion);
            Assert.Equal(new[] { "a.parquet" }, table.AddFiles.Select(x => x.Path));
        }

        [Fact]
        public async Task OldestVersionIsTheOldestUsableCheckpoint()
        {
            // Commits below 10 are gone, checkpoints 10 and 20 remain
            var storage = await CopyGoldenLog(nameof(OldestVersionIsTheOldestUsableCheckpoint), name => VersionOf(name) >= 10);

            var oldest = await DeltaTransactionReader.ReadTable(storage, GoldenTable, 0);
            await Truncate(storage, $"/{GoldenTable}/_delta_log/00000000000000000010.checkpoint.parquet");
            var oldestAfterTruncate = await DeltaTransactionReader.ReadTable(storage, GoldenTable, 0);

            Assert.NotNull(oldest);
            Assert.NotNull(oldestAfterTruncate);
            Assert.Equal(10, oldest.Version);
            Assert.Equal(20, oldestAfterTruncate.Version);
        }

        [Fact]
        public async Task MissingCommitOnlyBreaksTheVersionsThatNeedIt()
        {
            var storage = await CopyGoldenLog(nameof(MissingCommitOnlyBreaksTheVersionsThatNeedIt), name => VersionOf(name) >= 10 && !name.StartsWith("00000000000000000015.json"));

            var latest = await DeltaTransactionReader.ReadTable(storage, GoldenTable);
            var exception = await Assert.ThrowsAsync<DeltaVersionNotAvailableException>(() => DeltaTransactionReader.ReadTable(storage, GoldenTable, 17));

            Assert.NotNull(latest);
            Assert.Equal(20, latest.StartCheckpointVersion);
            Assert.Contains("commit 15 is missing", exception.Message);
        }

        [Fact]
        public async Task FailedCheckpointIsTheCauseWhenNothingElseCanBeRead()
        {
            var storage = await CopyGoldenLog(nameof(FailedCheckpointIsTheCauseWhenNothingElseCanBeRead), name => name.StartsWith("00000000000000000020."));
            await Truncate(storage, $"/{GoldenTable}/_delta_log/00000000000000000020.checkpoint.parquet");

            var exception = await Assert.ThrowsAsync<DeltaVersionNotAvailableException>(() => DeltaTransactionReader.ReadTable(storage, GoldenTable));

            Assert.NotNull(exception.InnerException);
        }

        [Fact]
        public async Task CheckpointThatFailedOnceIsUsedOnTheNextRead()
        {
            // A failed read can be transient, so the checkpoint is tried again
            var inner = await CopyGoldenLog(nameof(CheckpointThatFailedOnceIsUsedOnTheNextRead), _ => true);
            var storage = new OpenCountingStorage(inner) { FailNextOpenOf = "00000000000000000020.checkpoint.parquet" };
            // A logger, as the sink has one, so failures are recorded as reported
            var options = new DeltaReadOptions() { Logger = new CollectingLoggerProvider(), ReportedCheckpointFailures = new HashSet<string>() };

            var first = await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: options);
            var second = await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: options);

            Assert.NotNull(first);
            Assert.NotNull(second);
            Assert.Equal(10, first.StartCheckpointVersion);
            Assert.Equal(20, second.StartCheckpointVersion);
        }

        [Fact]
        public async Task CheckpointReadTimeoutFallsBack()
        {
            // HttpClient reports a timeout as a cancellation
            var inner = await CopyGoldenLog(nameof(CheckpointReadTimeoutFallsBack), _ => true);
            var storage = new OpenCountingStorage(inner)
            {
                FailNextOpenOf = "00000000000000000020.checkpoint.parquet",
                Failure = () => new TaskCanceledException("Simulated storage timeout")
            };

            var table = await DeltaTransactionReader.ReadTable(storage, GoldenTable);

            Assert.NotNull(table);
            Assert.Equal(10, table.StartCheckpointVersion);
        }

        [Fact]
        public async Task RepairedCheckpointIsUsedOnTheNextRead()
        {
            // Same size and no modification time, nothing but its content tells the repair apart
            var storage = await CopyGoldenLog(nameof(RepairedCheckpointIsUsedOnTheNextRead), _ => true);
            var path = $"/{GoldenTable}/_delta_log/00000000000000000020.checkpoint.parquet";
            var original = await ReadFileBytes(storage, path);
            var corrupted = (byte[])original.Clone();
            System.Array.Fill(corrupted, (byte)0, corrupted.Length / 4, corrupted.Length / 2);
            await WriteBytes(storage, path, corrupted);
            // A logger, as the sink has one, so failures are recorded as reported
            var options = new DeltaReadOptions() { Logger = new CollectingLoggerProvider(), ReportedCheckpointFailures = new HashSet<string>() };

            var broken = await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: options);
            await WriteBytes(storage, path, original);
            var repaired = await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: options);

            Assert.NotNull(broken);
            Assert.NotNull(repaired);
            Assert.Equal(10, broken.StartCheckpointVersion);
            Assert.Equal(20, repaired.StartCheckpointVersion);
        }

        [Fact]
        public async Task RepeatedCheckpointFailureIsWarnedOnce()
        {
            var storage = await CopyGoldenLog(nameof(RepeatedCheckpointFailureIsWarnedOnce), _ => true);
            await Truncate(storage, $"/{GoldenTable}/_delta_log/00000000000000000020.checkpoint.parquet");
            var logger = new CollectingLoggerProvider();
            var options = new DeltaReadOptions() { Logger = logger, ReportedCheckpointFailures = new HashSet<string>() };

            await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: options);
            await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: options);

            Assert.Single(logger.Messages, x => x.Contains("00000000000000000020.checkpoint.parquet"));
        }

        [Fact]
        public async Task ParsedRecordCountIsKeptWithoutJsonStatistics()
        {
            // Spark leaves out the JSON stats when delta.checkpoint.writeStatsAsJson is false
            var storage = Files.Of.InternalMemory($"./{nameof(ParsedRecordCountIsKeptWithoutJsonStatistics)}");
            await WriteClassicCheckpoint(storage, "t", 0, AddWithParsedStatistics,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "parsed.parquet", 1L, 0L, true, null, null, new object?[] { 42L, true } }),
                ("add", new object?[] { "json.parquet", 1L, 0L, true, "{\"numRecords\":7}", null, new object?[] { 99L, true } }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            var files = table.Files.ToDictionary(x => x.Action.Path!);
            Assert.Equal(42, files["parsed.parquet"].Statistics.NumRecords);
            Assert.Equal(7, files["json.parquet"].Statistics.NumRecords);
        }

        [Fact]
        public async Task GapInTheLogFailsTheRead()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(GapInTheLogFailsTheRead)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            await WriteCommit(storage, "t", 3, Add("d.parquet"));

            var exception = await Assert.ThrowsAsync<DeltaVersionNotAvailableException>(() => DeltaTransactionReader.ReadTable(storage, "t"));
            Assert.Contains("commit 2 is missing", exception.Message);
        }

        [Fact]
        public async Task UuidNamedCheckpointIsNotReplayedAsACommit()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(UuidNamedCheckpointIsNotReplayedAsACommit)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            await WriteText(storage, "/t/_delta_log/00000000000000000001.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json",
                "{\"checkpointMetadata\":{\"version\":1}}\n{\"add\":{\"path\":\"ghost.parquet\",\"partitionValues\":{},\"size\":1,\"modificationTime\":0,\"dataChange\":true}}\n");

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Equal(1, table.Version);
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table.AddFiles.Select(x => x.Path).Order());
        }

        [Fact]
        public async Task ReAddedFileReplacesTheEarlierAdd()
        {
            // Legal per spec, for example a statistics recompute
            var storage = Files.Of.InternalMemory($"./{nameof(ReAddedFileReplacesTheEarlierAdd)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet", "{\"numRecords\":1}"));
            await WriteCommit(storage, "t", 1, Add("a.parquet", "{\"numRecords\":2}", dataChange: false));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            var file = Assert.Single(table.Files);
            Assert.Equal(2, file.Statistics.NumRecords);
        }

        [Fact]
        public async Task TransactionsAndDomainsReconcileAcrossCommits()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(TransactionsAndDomainsReconcileAcrossCommits)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Txn("a", 5), Domain("kept", "old"), Domain("dropped", "x"));
            await WriteCommit(storage, "t", 1, Txn("a", 2), Txn("b", 7), Domain("kept", "new"), Domain("dropped", "x", removed: true));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            // The latest txn per application wins, even when its version is lower
            Assert.Equal(2, table.Transactions["a"].Version);
            Assert.Equal(7, table.Transactions["b"].Version);
            Assert.Equal(new[] { "kept" }, table.DomainMetadata.Keys);
            Assert.Equal("new", table.DomainMetadata["kept"].Configuration);
        }

        [Fact]
        public async Task CheckpointActionsReconcileWithLaterCommits()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointActionsReconcileWithLaterCommits)}");
            await WriteClassicCheckpoint(storage, "t", 1,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "a.parquet", 1L, 0L, false, "{\"numRecords\":3}" }),
                ("remove", new object?[] { "r.parquet", 5L, true, 1L }),
                ("txn", new object?[] { "app", 7L, 0L }),
                ("domainMetadata", new object?[] { "kept", "z", false }),
                ("domainMetadata", new object?[] { "dropped", "y", false }));
            await WriteCommit(storage, "t", 2, Add("b.parquet"), Remove("a.parquet"), Txn("app", 8), Domain("dropped", "y", removed: true));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Equal(1, table.StartCheckpointVersion);
            Assert.Equal(2, table.Version);
            Assert.Equal(new[] { "b.parquet" }, table.AddFiles.Select(x => x.Path));
            Assert.Equal(new[] { "a.parquet", "r.parquet" }, table.Tombstones.Select(x => x.Path).Order());
            var checkpointTombstone = table.Tombstones.Single(x => x.Path == "r.parquet");
            Assert.Equal(5, checkpointTombstone.DeletionTimestamp);
            Assert.Equal(1, checkpointTombstone.Size);
            Assert.True(checkpointTombstone.DataChange);
            Assert.Equal(8, table.Transactions["app"].Version);
            Assert.Equal(new[] { "kept" }, table.DomainMetadata.Keys);
        }

        [Fact]
        public async Task CheckpointAddsAreDataChanges()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointAddsAreDataChanges)}");
            await WriteClassicCheckpoint(storage, "t", 0,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "a.parquet", 1L, 0L, false, null }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.True(Assert.Single(table.AddFiles).DataChange);
        }

        [Fact]
        public async Task SkipTombstonesKeepsTheLiveFiles()
        {
            var storage = await CopyGoldenLog(nameof(SkipTombstonesKeepsTheLiveFiles), _ => true);

            var table = await DeltaTransactionReader.ReadTable(storage, GoldenTable);
            var skipped = await DeltaTransactionReader.ReadTable(storage, GoldenTable, options: new DeltaReadOptions() { SkipTombstones = true });

            Assert.NotNull(table);
            Assert.NotNull(skipped);
            Assert.NotEmpty(table.Tombstones);
            Assert.Empty(skipped.Tombstones);
            Assert.Equal(DescribeLive(table), DescribeLive(skipped));
        }

        [Fact]
        public async Task StatisticsAreParsedOnFirstUseOfFiles()
        {
            // Callers that only need the add actions never pay for statistics parsing
            var storage = Files.Of.InternalMemory($"./{nameof(StatisticsAreParsedOnFirstUseOfFiles)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet", "not json"));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Single(table.AddFiles);
            Assert.ThrowsAny<JsonException>(() => table.Files.Count);
        }

        [Fact]
        public async Task OnlyUnsupportedCheckpointsMeansTheTableCannotBeRead()
        {
            // Returning no table would let the sink create version 0 on top of an existing table
            var uuidOnly = Files.Of.InternalMemory($"./{nameof(OnlyUnsupportedCheckpointsMeansTheTableCannotBeRead)}_uuid");
            await WriteText(uuidOnly, "/t/_delta_log/00000000000000000005.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json", "{\"checkpointMetadata\":{\"version\":5}}\n");
            var multiPartOnly = Files.Of.InternalMemory($"./{nameof(OnlyUnsupportedCheckpointsMeansTheTableCannotBeRead)}_multipart");
            await WriteText(multiPartOnly, "/t/_delta_log/00000000000000000005.checkpoint.0000000001.0000000002.parquet", "not read");
            await WriteText(multiPartOnly, "/t/_delta_log/00000000000000000005.checkpoint.0000000002.0000000002.parquet", "not read");

            await Assert.ThrowsAsync<DeltaVersionNotAvailableException>(() => DeltaTransactionReader.ReadTable(uuidOnly, "t"));
            await Assert.ThrowsAsync<DeltaVersionNotAvailableException>(() => DeltaTransactionReader.ReadTable(multiPartOnly, "t"));
        }

        [Fact]
        public async Task OnlyHiddenLogFilesMeansNoTable()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(OnlyHiddenLogFilesMeansNoTable)}");
            await DeltaTransactionWriter.StageCommit(storage, "t", 0, new List<DeltaAction>() { Protocol(), Metadata(IdSchema) });

            Assert.Null(await DeltaTransactionReader.ReadTable(storage, "t"));
        }

        [Theory]
        [InlineData("protocol")]
        [InlineData("metaData")]
        public async Task CheckpointWithoutProtocolOrMetadataIsNotUsed(string missing)
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointWithoutProtocolOrMetadataIsNotUsed)}_{missing}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            await WriteCommit(storage, "t", 1, Add("b.parquet"));
            var rows = new List<(string Column, object?[] Values)>()
            {
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "ghost.parquet", 1L, 0L, true, null })
            };
            rows.RemoveAll(x => x.Column == missing);
            await WriteClassicCheckpoint(storage, "t", 1, rows.ToArray());

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Null(table.StartCheckpointVersion);
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table.AddFiles.Select(x => x.Path).Order());
        }

        [Fact]
        public async Task CheckpointRowsKeepTheirOwnNullFields()
        {
            // One visitor reads every cell, a value from the previous row must not leak into the next
            var storage = Files.Of.InternalMemory($"./{nameof(CheckpointRowsKeepTheirOwnNullFields)}");
            await WriteClassicCheckpoint(storage, "t", 0,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", IdSchema, 0L }),
                ("add", new object?[] { "a.parquet", 1L, 10L, true, "{\"numRecords\":1}" }),
                ("add", new object?[] { "b.parquet", 2L, null, true, null }),
                ("add", new object?[] { "c.parquet", 3L, 30L, true, "{\"numRecords\":3}" }),
                ("remove", new object?[] { "r.parquet", 5L, true, 4L }),
                ("remove", new object?[] { "s.parquet", null, true, null }),
                ("txn", new object?[] { "app", 7L, 70L }),
                ("txn", new object?[] { "other", 8L, null }));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            var adds = table.AddFiles.ToDictionary(x => x.Path!);
            Assert.Equal("{\"numRecords\":1}", adds["a.parquet"].Statistics);
            Assert.Null(adds["b.parquet"].Statistics);
            Assert.Equal(0, adds["b.parquet"].ModificationTime);
            Assert.Equal(2, adds["b.parquet"].Size);
            Assert.Equal("{\"numRecords\":3}", adds["c.parquet"].Statistics);
            Assert.Equal(30, adds["c.parquet"].ModificationTime);
            var tombstones = table.Tombstones.ToDictionary(x => x.Path!);
            Assert.Equal(5, tombstones["r.parquet"].DeletionTimestamp);
            Assert.Null(tombstones["s.parquet"].DeletionTimestamp);
            Assert.Null(tombstones["s.parquet"].Size);
            Assert.Equal(70, table.Transactions["app"].LastUpdated);
            Assert.Null(table.Transactions["other"].LastUpdated);
        }

        [Fact]
        public async Task RemovedDomainCanBeAddedAgain()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(RemovedDomainCanBeAddedAgain)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Domain("d", "first"));
            await WriteCommit(storage, "t", 1, Domain("d", "first", removed: true));
            await WriteCommit(storage, "t", 2, Domain("d", "second"));

            var removed = await DeltaTransactionReader.ReadTable(storage, "t", 1);
            var readded = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(removed);
            Assert.NotNull(readded);
            Assert.Empty(removed.DomainMetadata);
            Assert.Equal("second", readded.DomainMetadata["d"].Configuration);
        }

        [Fact]
        public async Task FilesAreParsedOnce()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(FilesAreParsedOnce)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet", "{\"numRecords\":4}"));

            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.NotNull(table);
            Assert.Same(table.Files, table.Files);
            Assert.Equal(4, Assert.Single(table.Files).Statistics.NumRecords);
        }

        [Fact]
        public void DeletionVectorPathFollowsTheSpecExample()
        {
            var deletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^", Offset = 4, SizeInBytes = 40, Cardinality = 6 };

            Assert.Equal("ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", deletionVector.AbsolutePath);
        }

        [Fact]
        public void DeletionVectorPathWithoutPrefixIsInTheTableRoot()
        {
            var guid = Guid.Parse("d2c639aa-8816-431a-aaf6-d3fe2512ff61");
            var deletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = Z85.EncodeGuid(guid), SizeInBytes = 40, Cardinality = 6 };

            Assert.Equal($"deletion_vector_{guid}.bin", deletionVector.AbsolutePath);
        }

        [Theory]
        [InlineData("interval 7 days", "7.00:00:00")]
        [InlineData("interval 1 week", "7.00:00:00")]
        [InlineData("interval 2 weeks", "14.00:00:00")]
        [InlineData("30 days", "30.00:00:00")]
        [InlineData("INTERVAL 168 HOURS", "7.00:00:00")]
        [InlineData("interval 1 day 12 hours", "1.12:00:00")]
        [InlineData("interval 90 minutes", "01:30:00")]
        [InlineData("interval 30 seconds", "00:00:30")]
        [InlineData("interval 1 millisecond", "00:00:00.001")]
        [InlineData("interval 1 microsecond", "00:00:00.000001")]
        public void IntervalParsesSparkForms(string value, string expected)
        {
            Assert.Equal(TimeSpan.Parse(expected, CultureInfo.InvariantCulture), DeltaInterval.Parse(value));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("7")]
        [InlineData("days")]
        [InlineData("interval")]
        [InlineData("interval 1 month")]
        [InlineData("interval 1 year")]
        [InlineData("interval -1 days")]
        [InlineData("interval 1.5 days")]
        [InlineData("interval 7 days extra")]
        [InlineData("interval 99999999999999999999 days")]
        public void IntervalRejectsOtherForms(string? value)
        {
            Assert.Null(DeltaInterval.Parse(value));
        }

        [Fact]
        public async Task HiddenLogFilesAreListedButNotReplayed()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(HiddenLogFilesAreListedButNotReplayed)}");
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(IdSchema), Add("a.parquet"));
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 1, new List<DeltaAction>() { Add("b.parquet") });

            var hidden = await DeltaTransactionReader.ListHiddenLogFiles(storage, "t");
            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.Equal(staged, Assert.Single(hidden).Name);
            Assert.NotNull(table);
            Assert.Equal(0, table.Version);
        }

        [Fact]
        public async Task VersionCommitIncludesCommitInfo()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(VersionCommitIncludesCommitInfo)}");
            var commitInfo = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "WRITE" } } };
            await WriteCommit(storage, "t", 0, commitInfo, Protocol(), Metadata(IdSchema));

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0);

            Assert.NotNull(commit);
            Assert.NotNull(commit.CommitInfo);
            Assert.Equal("WRITE", commit.CommitInfo.Data!["operation"].ToString());
        }

        [Fact]
        public async Task SinkCheckpointsMatchFullReplay()
        {
            // The sink reads its own checkpoints back on every commit
            var storage = Files.Of.InternalMemory($"./{nameof(SinkCheckpointsMatchFullReplay)}");
            await using var stream = new DeltaLakeSinkStream(nameof(SinkCheckpointsMatchFullReplay), storage, options =>
            {
                options.CheckpointInterval = 2;
            });
            stream.Generate(100);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    name STRING
                );

                INSERT INTO test
                SELECT userKey, firstName AS name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);
            for (int version = 1; version <= 5; version++)
            {
                // Each delete re-adds the file with a new deletion vector
                stream.DeleteUser(stream.Users[0]);
                await WaitForVersion(storage, "test", stream, version);
            }

            var jsonOnly = await CopyLog(storage, "test", $"{nameof(SinkCheckpointsMatchFullReplay)}_json", IsNotCheckpoint);
            var table = await DeltaTransactionReader.ReadTable(storage, "test");
            var replay = await DeltaTransactionReader.ReadTable(jsonOnly, "test");

            Assert.NotNull(table);
            Assert.NotNull(replay);
            Assert.Equal(4, table.StartCheckpointVersion);
            Assert.Contains(table.AddFiles, x => x.DeletionVector != null);
            // Flowtide checkpoints do not carry tombstones before Phase 3
            Assert.Equal(DescribeLive(replay), DescribeLive(table));
        }

        [Theory]
        [InlineData("txn")]
        [InlineData("domain")]
        public async Task SinkSkipsCheckpointsWhileTheTableHoldsTransactionsOrDomains(string kept)
        {
            // The checkpoint writer would drop them, so no checkpoint is written
            var testName = $"{nameof(SinkSkipsCheckpointsWhileTheTableHoldsTransactionsOrDomains)}_{kept}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema), kept == "txn" ? Txn("external-app", 3) : Domain("external.domain", "{}"));

            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 2;
            });
            var warnings = new CollectingLoggerProvider();
            stream.AddLoggerProvider(warnings);
            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    name STRING
                );

                INSERT INTO test
                SELECT userKey AS userkey, firstName AS name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 1);
            // Checkpoints are due at versions 2 and 4
            for (int version = 2; version <= 5; version++)
            {
                stream.DeleteUser(stream.Users[0]);
                await WaitForVersion(storage, "test", stream, version);
            }

            var logFiles = await storage.Ls("/test/_delta_log/");
            Assert.DoesNotContain(logFiles, x => x.Name.Contains(".checkpoint."));
            var table = await DeltaTransactionReader.ReadTable(storage, "test");
            Assert.NotNull(table);
            if (kept == "txn")
            {
                Assert.Equal(3, table.Transactions["external-app"].Version);
            }
            else
            {
                Assert.Equal("{}", table.DomainMetadata["external.domain"].Configuration);
            }
            Assert.Single(warnings.Messages, x => x.Contains("no checkpoint is written"));
        }

        private sealed class CollectingLoggerProvider : Microsoft.Extensions.Logging.ILoggerProvider, Microsoft.Extensions.Logging.ILogger
        {
            private readonly System.Collections.Concurrent.ConcurrentQueue<string> _messages = new System.Collections.Concurrent.ConcurrentQueue<string>();

            public IReadOnlyCollection<string> Messages => _messages.ToArray();

            public Microsoft.Extensions.Logging.ILogger CreateLogger(string categoryName) => this;

            public void Dispose()
            {
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel >= Microsoft.Extensions.Logging.LogLevel.Warning;

            public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= Microsoft.Extensions.Logging.LogLevel.Warning)
                {
                    _messages.Enqueue(formatter(state, exception));
                }
            }
        }

        [Fact]
        public async Task SinkCheckpointKeepsTheParsedRecordCountOfDeletionVectorFiles()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(SinkCheckpointKeepsTheParsedRecordCountOfDeletionVectorFiles)}");
            await WriteClassicCheckpoint(storage, "test", 0, AddWithParsedStatistics,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", UserSchema, 0L }),
                ("add", new object?[] { "old.parquet", 1L, 0L, true, null, DeletionVectorRow(), new object?[] { 42L, true } }));

            await AppendUntilVersion(storage, nameof(SinkCheckpointKeepsTheParsedRecordCountOfDeletionVectorFiles), 3, null);

            var table = await DeltaTransactionReader.ReadTable(storage, "test");
            Assert.NotNull(table);
            Assert.Equal(2, table.StartCheckpointVersion);
            var file = table.Files.Single(x => x.Action.Path == "old.parquet");
            Assert.NotNull(file.Action.DeletionVector);
            Assert.Equal(42, file.Statistics.NumRecords);
        }

        [Theory]
        [InlineData(0, null, false)]
        [InlineData(1, "{\"minValues\":{\"numRecords\":1}}", false)]
        [InlineData(2, "{\"numRecords\":null}", false)]
        [InlineData(3, "{\"numRecords\":-1}", false)]
        [InlineData(4, "{\"numRecords\":5}", true)]
        public async Task SinkSkipsCheckpointsWhileDeletionVectorFilesLackARecordCount(int id, string? statistics, bool checkpointExpected)
        {
            // The spec requires numRecords on every file with a deletion vector, a checkpoint without it is invalid
            var testName = $"{nameof(SinkSkipsCheckpointsWhileDeletionVectorFilesLackARecordCount)}_{id}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteClassicCheckpoint(storage, "test", 0, AddWithParsedStatistics,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", UserSchema, 0L }),
                ("add", new object?[] { "old.parquet", 1L, 0L, true, statistics, DeletionVectorRow(), null }));
            var warnings = new CollectingLoggerProvider();

            // Checkpoints are due at versions 2 and 4
            await AppendUntilVersion(storage, testName, 5, warnings);

            var logFiles = await storage.Ls("/test/_delta_log/");
            var checkpoints = logFiles.Where(x => x.Name.Contains(".checkpoint.")).Select(x => x.Name).Order().ToList();
            if (checkpointExpected)
            {
                Assert.Equal(new[] { "00000000000000000000.checkpoint.parquet", "00000000000000000002.checkpoint.parquet", "00000000000000000004.checkpoint.parquet" }, checkpoints);
                Assert.DoesNotContain(warnings.Messages, x => x.Contains("deletion vectors but no record count"));
            }
            else
            {
                Assert.Equal(new[] { "00000000000000000000.checkpoint.parquet" }, checkpoints);
                Assert.Single(warnings.Messages, x => x.Contains("deletion vectors but no record count"));
            }
        }

        private static async Task AppendUntilVersion(IFileStorage storage, string testName, long version, CollectingLoggerProvider? logger)
        {
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.CheckpointInterval = 2;
            });
            if (logger != null)
            {
                stream.AddLoggerProvider(logger);
            }
            stream.Generate(5);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    name STRING
                );

                INSERT INTO test
                SELECT userKey AS userkey, firstName AS name FROM users
            ");

            await WaitForVersion(storage, "test", stream, 1);
            for (long next = 2; next <= version; next++)
            {
                stream.Generate(1);
                await WaitForVersion(storage, "test", stream, next);
            }
        }

        private static object?[] DeletionVectorRow()
        {
            return new object?[] { "u", Z85.EncodeGuid(Guid.Parse("d2c639aa-8816-431a-aaf6-d3fe2512ff61")), 1, 40, 1L };
        }

        // Adds with a deletion vector and parsed statistics
        private static Dictionary<string, (string Name, IArrowType Type)[]> AddWithParsedStatistics => new Dictionary<string, (string Name, IArrowType Type)[]>(CheckpointColumns)
        {
            ["add"] = new (string, IArrowType)[]
            {
                ("path", StringType.Default),
                ("size", Int64Type.Default),
                ("modificationTime", Int64Type.Default),
                ("dataChange", BooleanType.Default),
                ("stats", StringType.Default),
                ("deletionVector", new StructType(new List<Field>()
                {
                    new Field("storageType", StringType.Default, true),
                    new Field("pathOrInlineDv", StringType.Default, true),
                    new Field("offset", Int32Type.Default, true),
                    new Field("sizeInBytes", Int32Type.Default, true),
                    new Field("cardinality", Int64Type.Default, true)
                })),
                ("stats_parsed", new StructType(new List<Field>()
                {
                    new Field("numRecords", Int64Type.Default, true),
                    new Field("tightBounds", BooleanType.Default, true)
                }))
            }
        };

        private static bool IsNotCheckpoint(string name)
        {
            return !name.Contains(".checkpoint.");
        }

        private static long VersionOf(string name)
        {
            return name.Length > 20 && long.TryParse(name.AsSpan(0, 20), out var version) ? version : -1;
        }

        private sealed class OpenCountingStorage : IFileStorage
        {
            private readonly IFileStorage _inner;
            public OpenCountingStorage(IFileStorage inner)
            {
                _inner = inner;
            }

            // The next open of a path ending with this fails once
            public string? FailNextOpenOf { get; set; }

            public Func<Exception>? Failure { get; set; }

            public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
            {
                if (FailNextOpenOf != null && path.Full.EndsWith(FailNextOpenOf))
                {
                    FailNextOpenOf = null;
                    throw Failure?.Invoke() ?? new IOException($"Simulated failure opening {path.Full}");
                }
                return _inner.OpenRead(path, cancellationToken);
            }

            public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => _inner.OpenWrite(path, cancellationToken);
            public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => _inner.Ren(name, newName, cancellationToken);
            public Task<string?> ReadText(IOPath path, System.Text.Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.ReadText(path, encoding, cancellationToken);
            public Task WriteText(IOPath path, string contents, System.Text.Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.WriteText(path, contents, encoding, cancellationToken);
            public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => _inner.Ls(path, recurse, cancellationToken);
            public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => _inner.ReadAsJson<T>(path, cancellationToken);
            public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => _inner.WriteAsJson(path, value, writeIndented, cancellationToken);
            public Task Rm(IOPath path, CancellationToken cancellationToken = default) => _inner.Rm(path, cancellationToken);
            public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => _inner.Exists(path, cancellationToken);
            public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => _inner.Stat(path, cancellationToken);

            public void Dispose()
            {
            }
        }

        private static List<string> Describe(DeltaTable table)
        {
            var lines = DescribeLive(table);
            // Checkpoints need not keep a remove's stats and tags, Spark's leave out defaultRowCommitVersion and write dataChange false
            lines.AddRange(table.Tombstones.Select(x =>
                $"remove {x.Path} {x.DeletionTimestamp} {x.ExtendedFileMetadata} {Format(x.PartitionValues)} {x.Size} {Format(x.DeletionVector)} {x.BaseRowId}"));
            lines.Sort(StringComparer.Ordinal);
            return lines;
        }

        private static List<string> DescribeLive(DeltaTable table)
        {
            var metadata = table.Metadata;
            var protocol = table.Protocol;
            var lines = new List<string>()
            {
                $"metadata {metadata.Id} {metadata.Name} {metadata.Description} {metadata.Format?.Provider} {Format(metadata.Format?.Options)} {metadata.SchemaString} {string.Join(",", metadata.PartitionColumns ?? new List<string>())} {Format(metadata.Configuration)} {metadata.CreatedTime}",
                $"protocol {protocol.MinReaderVersion} {protocol.MinWriterVersion} {FormatList(protocol.ReaderFeatures)} {FormatList(protocol.WriterFeatures)}"
            };
            // dataChange is left out, checkpoint adds always count as data
            lines.AddRange(table.AddFiles.Select(x =>
                $"add {x.Path} {Format(x.PartitionValues)} {x.Size} {x.ModificationTime} {x.Statistics} {Format(x.Tags)} {Format(x.DeletionVector)} {x.BaseRowId} {x.DefaultRowCommitVersion} {x.ClusteringProvider}"));
            lines.AddRange(table.Transactions.Select(x => $"txn {x.Key} {x.Value.Version} {x.Value.LastUpdated}"));
            lines.AddRange(table.DomainMetadata.Select(x => $"domain {x.Key} {x.Value.Configuration}"));
            lines.Sort(StringComparer.Ordinal);
            return lines;
        }

        private static string FormatList(IReadOnlyList<string>? values)
        {
            return values == null ? "null" : "[" + string.Join(",", values) + "]";
        }

        private static string Format(IReadOnlyDictionary<string, string>? values)
        {
            if (values == null || values.Count == 0)
            {
                return "{}";
            }
            return "{" + string.Join(",", values.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => $"{x.Key}={x.Value}")) + "}";
        }

        private static string Format(DeletionVector? deletionVector)
        {
            if (deletionVector == null)
            {
                return "no-dv";
            }
            return $"dv {deletionVector.StorageType} {deletionVector.PathOrInlineDv} {deletionVector.Offset} {deletionVector.SizeInBytes} {deletionVector.Cardinality}";
        }

        private static Task<IFileStorage> CopyGoldenLog(string storageName, Func<string, bool> include)
        {
            return CopyLog(Files.Of.LocalDisk("../../../testdata"), GoldenTable, storageName, include);
        }

        private static async Task<IFileStorage> CopyLog(IFileStorage source, string table, string storageName, Func<string, bool> include)
        {
            var target = Files.Of.InternalMemory($"./{storageName}");
            foreach (var entry in await source.Ls($"/{table}/_delta_log/"))
            {
                if (entry.Path.IsFolder || !include(entry.Name))
                {
                    continue;
                }
                using var read = await source.OpenRead(entry.Path);
                Assert.NotNull(read);
                using var memory = new MemoryStream();
                await read.CopyToAsync(memory);
                using var write = await target.OpenWrite($"/{table}/_delta_log/{entry.Name}");
                Assert.NotNull(write);
                await write.WriteAsync(memory.ToArray());
            }
            return target;
        }

        private static async Task Truncate(IFileStorage storage, string path)
        {
            byte[] bytes;
            using (var read = await storage.OpenRead(path))
            {
                Assert.NotNull(read);
                using var memory = new MemoryStream();
                await read.CopyToAsync(memory);
                bytes = memory.ToArray();
            }
            using var write = await storage.OpenWrite(path);
            Assert.NotNull(write);
            await write.WriteAsync(bytes.AsMemory(0, bytes.Length / 2));
        }

        private static async Task<byte[]> ReadFileBytes(IFileStorage storage, string path)
        {
            using var read = await storage.OpenRead(path);
            Assert.NotNull(read);
            using var memory = new MemoryStream();
            await read.CopyToAsync(memory);
            return memory.ToArray();
        }

        private static async Task WriteBytes(IFileStorage storage, string path, byte[] bytes)
        {
            using var write = await storage.OpenWrite(path);
            Assert.NotNull(write);
            await write.WriteAsync(bytes);
        }

        private static async Task WriteText(IFileStorage storage, string path, string text)
        {
            using var write = await storage.OpenWrite(path);
            Assert.NotNull(write);
            await write.WriteAsync(System.Text.Encoding.UTF8.GetBytes(text));
        }

        private static Task WriteCommit(IFileStorage storage, string table, long version, params DeltaAction[] actions)
        {
            return DeltaTransactionWriter.WriteCommit(storage, table, version, actions.ToList());
        }

        private static DeltaAction Protocol()
        {
            return new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 } };
        }

        private static DeltaAction Metadata(string schema)
        {
            return new DeltaAction()
            {
                MetaData = new DeltaMetadataAction()
                {
                    Id = "table-id",
                    SchemaString = schema,
                    PartitionColumns = new List<string>(),
                    Format = new DeltaMetadataFormat() { Provider = "parquet", Options = new Dictionary<string, string>() },
                    Configuration = new Dictionary<string, string>()
                }
            };
        }

        private static DeltaAction Add(string path, string? stats = null, bool dataChange = true)
        {
            return new DeltaAction() { Add = new DeltaAddAction() { Path = path, Size = 1, DataChange = dataChange, PartitionValues = new Dictionary<string, string>(), Statistics = stats } };
        }

        private static DeltaAction Remove(string path)
        {
            return new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = path, DataChange = true, DeletionTimestamp = 1 } };
        }

        private static DeltaAction Txn(string appId, long version)
        {
            return new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = appId, Version = version } };
        }

        private static DeltaAction Domain(string domain, string configuration, bool removed = false)
        {
            return new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = domain, Configuration = configuration, Removed = removed } };
        }

        // Child fields of each top level checkpoint column, in the order rows list their values
        private static readonly Dictionary<string, (string Name, IArrowType Type)[]> CheckpointColumns = new()
        {
            ["protocol"] = new (string, IArrowType)[] { ("minReaderVersion", Int32Type.Default), ("minWriterVersion", Int32Type.Default) },
            ["metaData"] = new (string, IArrowType)[] { ("id", StringType.Default), ("schemaString", StringType.Default), ("createdTime", Int64Type.Default) },
            ["add"] = new (string, IArrowType)[] { ("path", StringType.Default), ("size", Int64Type.Default), ("modificationTime", Int64Type.Default), ("dataChange", BooleanType.Default), ("stats", StringType.Default) },
            ["remove"] = new (string, IArrowType)[] { ("path", StringType.Default), ("deletionTimestamp", Int64Type.Default), ("dataChange", BooleanType.Default), ("size", Int64Type.Default) },
            ["txn"] = new (string, IArrowType)[] { ("appId", StringType.Default), ("version", Int64Type.Default), ("lastUpdated", Int64Type.Default) },
            ["domainMetadata"] = new (string, IArrowType)[] { ("domain", StringType.Default), ("configuration", StringType.Default), ("removed", BooleanType.Default) },
            ["sidecar"] = new (string, IArrowType)[] { ("path", StringType.Default), ("sizeInBytes", Int64Type.Default), ("modificationTime", Int64Type.Default) },
            ["checkpointMetadata"] = new (string, IArrowType)[] { ("version", Int64Type.Default) },
            ["commitInfo"] = new (string, IArrowType)[] { ("operation", StringType.Default) },
        };

        private static Task WriteClassicCheckpoint(IFileStorage storage, string table, long version, params (string Column, object?[] Values)[] rows)
        {
            return WriteClassicCheckpoint(storage, table, version, CheckpointColumns, rows);
        }

        private static async Task WriteClassicCheckpoint(IFileStorage storage, string table, long version, IReadOnlyDictionary<string, (string Name, IArrowType Type)[]> columns, params (string Column, object?[] Values)[] rows)
        {
            var fields = new List<Field>();
            var arrays = new List<IArrowArray>();
            foreach (var column in rows.Select(x => x.Column).Distinct())
            {
                var childFields = columns[column].Select(x => new Field(x.Name, x.Type, true)).ToList();
                var children = new List<IArrowArray>();
                for (int c = 0; c < childFields.Count; c++)
                {
                    children.Add(BuildArray(childFields[c].DataType, rows.Select(x => x.Column == column ? x.Values[c] : null)));
                }
                var validity = new ArrowBuffer.BitmapBuilder();
                int nullCount = 0;
                foreach (var row in rows)
                {
                    validity.Append(row.Column == column);
                    if (row.Column != column)
                    {
                        nullCount++;
                    }
                }
                var structType = new StructType(childFields);
                arrays.Add(new StructArray(structType, rows.Length, children, validity.Build(), nullCount));
                fields.Add(new Field(column, structType, true));
            }

            var schema = new Apache.Arrow.Schema(fields, null);
            using var batch = new RecordBatch(schema, arrays, rows.Length);
            using var memory = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(memory, schema))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            using var target = await storage.OpenWrite($"/{table}/_delta_log/{version:D20}.checkpoint.parquet");
            Assert.NotNull(target);
            await target.WriteAsync(memory.ToArray());
        }

        private static IArrowArray BuildArray(IArrowType type, IEnumerable<object?> values)
        {
            switch (type)
            {
                case StringType:
                    {
                        var builder = new StringArray.Builder();
                        foreach (var value in values)
                        {
                            if (value == null) builder.AppendNull(); else builder.Append((string)value);
                        }
                        return builder.Build();
                    }
                case Int64Type:
                    {
                        var builder = new Int64Array.Builder();
                        foreach (var value in values)
                        {
                            if (value == null) builder.AppendNull(); else builder.Append(Convert.ToInt64(value));
                        }
                        return builder.Build();
                    }
                case Int32Type:
                    {
                        var builder = new Int32Array.Builder();
                        foreach (var value in values)
                        {
                            if (value == null) builder.AppendNull(); else builder.Append(Convert.ToInt32(value));
                        }
                        return builder.Build();
                    }
                case StructType structType:
                    {
                        var rows = values.ToList();
                        var children = new List<IArrowArray>();
                        for (int c = 0; c < structType.Fields.Count; c++)
                        {
                            children.Add(BuildArray(structType.Fields[c].DataType, rows.Select(x => x == null ? null : ((object?[])x)[c])));
                        }
                        var validity = new ArrowBuffer.BitmapBuilder();
                        foreach (var row in rows)
                        {
                            validity.Append(row != null);
                        }
                        return new StructArray(structType, rows.Count, children, validity.Build(), rows.Count(x => x == null));
                    }
                case DoubleType:
                    {
                        var builder = new DoubleArray.Builder();
                        foreach (var value in values)
                        {
                            if (value == null) builder.AppendNull(); else builder.Append(Convert.ToDouble(value));
                        }
                        return builder.Build();
                    }
                case BooleanType:
                    {
                        var builder = new BooleanArray.Builder();
                        foreach (var value in values)
                        {
                            if (value == null) builder.AppendNull(); else builder.Append((bool)value);
                        }
                        return builder.Build();
                    }
                default:
                    throw new NotSupportedException(type.Name);
            }
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
