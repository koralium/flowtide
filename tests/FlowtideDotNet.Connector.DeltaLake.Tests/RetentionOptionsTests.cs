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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RetentionOptionsTests
    {
        private static readonly TimeSpan s_retention = TimeSpan.FromDays(7);

        private const string Published = "interval 168 hours";

        // A sink's own temporary, as the staged sweep recognizes it
        private const string Temporary = ".flowtide-0f8fad5bd9cb469fa16570867728950e.checkpoint.tmp";

        // Seven days on an in-memory store, which the assertion makes eligible
        private static void Retained(DeltaLakeOptions options)
        {
            options.HistoryRetention = s_retention;
            options.AssumeCompleteStorageListing = true;
        }

        // A table another writer created, without the retention properties
        private static async Task<IFileStorage> ExistingTable(string testName, Dictionary<string, string>? configuration = null, List<string>? partitionColumns = null)
        {
            var storage = Files.Of.InternalMemory($"./{testName}");
            var metadata = Metadata(UserSchema, configuration);
            if (partitionColumns != null)
            {
                metadata.MetaData!.PartitionColumns = partitionColumns;
            }
            await WriteCommit(storage, "test", 0, Protocol(), metadata);
            return storage;
        }

        private static DeltaLakeSink Construct(DeltaLakeOptions options, string table = "t")
        {
            return new DeltaLakeSink(options, CleanupConfigurationTests.Relation(table), new ExecutionDataflowBlockOptions());
        }

        private static async Task<Dictionary<string, string>?> TableConfiguration(IFileStorage storage)
        {
            return (await DeltaTransactionReader.ReadTable(storage, "test"))!.Metadata.Configuration;
        }

        [Fact]
        public void InvalidRetentionThrowsAtConstructionWithRemedy()
        {
            var directory = StoreFixtures.TempDirectory(nameof(InvalidRetentionThrowsAtConstructionWithRemedy));
            using var storage = Files.Of.LocalDisk(directory);
            foreach (var retention in new[] { TimeSpan.FromHours(23), TimeSpan.FromDays(1) + TimeSpan.FromMinutes(30), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1), TimeSpan.Zero, TimeSpan.FromDays(-7), TimeSpan.MaxValue })
            {
                var options = new DeltaLakeOptions() { StorageLocation = storage, HistoryRetention = retention };
                var e = Assert.Throws<ArgumentException>(() => Construct(options));
                Assert.Equal(nameof(DeltaLakeOptions.HistoryRetention), e.ParamName);
                Assert.Contains("use whole hours of at least one day", e.Message);
                Assert.Contains("disable history retention", e.Message);
                // The factory every stream uses funnels through the same constructor
                Assert.Throws<ArgumentException>(() => new DeltaLakeSinkFactory(options).CreateSink(CleanupConfigurationTests.Relation("t"), null!, new ExecutionDataflowBlockOptions()));
            }
            // The largest whole-hours value constructs
            var largest = TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour);
            foreach (var retention in new[] { TimeSpan.FromHours(24), TimeSpan.FromHours(25), s_retention, largest })
            {
                var cleanup = Construct(new DeltaLakeOptions() { StorageLocation = storage, HistoryRetention = retention }).CleanupForTests;
                Assert.Equal(retention, cleanup.OptIns.Retention);
                Assert.True(cleanup.Store.Eligible);
            }
            Assert.Null(Construct(new DeltaLakeOptions() { StorageLocation = storage }).CleanupForTests.OptIns.Retention);
        }

        [Fact]
        public async Task RetentionStoreValidationPrecedesInitialization()
        {
            var testName = nameof(RetentionStoreValidationPrecedesInitialization);
            // An unknown wrapper nobody vouched for
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.HistoryRetention = s_retention);
            stream.Generate(5);
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => stream.StartStream(UserInsert));
            var refused = Assert.IsType<ArgumentException>(Refusal(failure));
            Assert.Equal(CleanupConfiguration.AssertionOption, refused.ParamName);

            // Refused before the sink existed: no request, no worker, no initialization
            Assert.Empty(storage.Requests);
            Assert.Empty(stream.Sinks);
            Assert.Empty(stream.Maintenance.Workers);

            // The retention value is checked before the store
            using var gcs = StoreFixtures.Gcs();
            var invalid = Assert.Throws<ArgumentException>(() => Construct(new DeltaLakeOptions() { StorageLocation = gcs, HistoryRetention = TimeSpan.FromHours(1) }, "bucket/t"));
            Assert.Equal(nameof(DeltaLakeOptions.HistoryRetention), invalid.ParamName);
            var ineligible = Assert.Throws<ArgumentException>(() => Construct(new DeltaLakeOptions() { StorageLocation = gcs, HistoryRetention = s_retention }, "bucket/t"));
            Assert.Equal(CleanupConfiguration.StorageOption, ineligible.ParamName);
        }

        // The configuration exception, through any engine wrappers
        private static Exception Refusal(Exception e)
        {
            Exception? current = e;
            while (current != null && current is not ArgumentException)
            {
                current = current.InnerException;
            }
            return current ?? e;
        }

        [Fact]
        public async Task ValidatedCleanupConfigurationSurvivesEngineRestart()
        {
            var testName = nameof(ValidatedCleanupConfigurationSurvivesEngineRestart);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var sinks = new ConcurrentQueue<DeltaLakeSink>();
            DeltaLakeOptions? options = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, configure =>
            {
                options = configure;
                Retained(configure);
                configure.SinkCreatedForTests = sinks.Enqueue;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            var sink = Assert.Single(sinks);
            var cleanup = sink.CleanupForTests;

            // Options changed after construction, an invalid retention included, are not validated or read again
            options!.HistoryRetention = TimeSpan.FromHours(1);
            options.AssumeCompleteStorageListing = false;
            await stream.Crash();
            stream.Generate(1);
            await WaitForVersion(storage.Inner, "test", stream, 1);
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));

            Assert.Same(sink, Assert.Single(sinks));
            Assert.Same(cleanup, sink.CleanupForTests);
            Assert.Equal(s_retention, cleanup.OptIns.Retention);
            Assert.True(cleanup.Store.Asserted);
            // The restarted worker and the commits keep the construction snapshot
            Assert.Equal(s_retention, sink.WorkerSettingsForTests!.Retention);
            Assert.DoesNotContain(await ReadCommitActions(storage, "test", 1), x => x.MetaData != null);
            Assert.True(RetentionProperties.Matches(await TableConfiguration(storage), Published));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RetentionInspectionNeedsAnEligibleStorage(bool replaced)
        {
            var testName = nameof(RetentionInspectionNeedsAnEligibleStorage) + replaced;
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                Retained(options);
                options.CheckpointInterval = 0;
                if (replaced)
                {
                    // Another instance after the classification at construction
                    options.SinkCreatedForTests = _ => options.StorageLocation = new ForwardingStorage(storage);
                }
            });
            stream.Generate(3);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // Periodic builds off: inspection follows retention only on an eligible storage
            var settings = stream.Sinks.Single().WorkerSettingsForTests!;
            Assert.False(settings.PeriodicEnabled);
            Assert.Equal(s_retention, settings.Retention);
            Assert.Equal(!replaced, settings.CleanupEligible);
            Assert.Equal(!replaced, settings.InspectionEnabled);
        }

        [Fact]
        public async Task NewTableCreatedWithRetentionProperties()
        {
            var testName = nameof(NewTableCreatedWithRetentionProperties);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            var metaData = Assert.Single(await ReadCommitActions(storage, "test", 0), x => x.MetaData != null).MetaData!;
            Assert.Equal(Published, metaData.Configuration![RetentionProperties.LogKey]);
            Assert.Equal(Published, metaData.Configuration[RetentionProperties.DeletedFileKey]);
            // The table needs no properties commit of its own
            await RunCheckpoints(stream, 3);
            Assert.False(await storage.Exists(CommitPath("test", 1)));
        }

        [Fact]
        public async Task PropertiesCommitOnIdleTable()
        {
            var testName = nameof(PropertiesCommitOnIdleTable);
            var storage = await ExistingTable(testName, new Dictionary<string, string>() { ["custom"] = "kept" });
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            await WaitForVersionCheckpointing(storage, "test", stream, 1);

            // commitInfo first, then the copied metaData, and no file
            var actions = await ReadCommitActions(storage, "test", 1);
            Assert.Equal(2, actions.Count);
            Assert.Equal("SET TBLPROPERTIES", actions[0].CommitInfo!.Operation);
            Assert.NotNull(actions[0].CommitInfo!.StageId);
            var configuration = actions[1].MetaData!.Configuration!;
            Assert.Equal("kept", configuration["custom"]);
            Assert.Equal(Published, configuration[RetentionProperties.LogKey]);
            Assert.Equal(Published, configuration[RetentionProperties.DeletedFileKey]);
            Assert.True(RetentionProperties.Matches(await TableConfiguration(storage), Published));

            await RunCheckpoints(stream, 3);
            Assert.False(await storage.Exists(CommitPath("test", 2)));
        }

        [Fact]
        public async Task PropertiesMergedIntoDataCommit()
        {
            var testName = nameof(PropertiesMergedIntoDataCommit);
            var storage = await ExistingTable(testName);
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);

            var actions = await ReadCommitActions(storage, "test", 1);
            Assert.Equal("WRITE", actions[0].CommitInfo!.Operation);
            Assert.NotNull(Assert.Single(actions, x => x.MetaData != null).MetaData);
            Assert.Null(actions[0].MetaData);
            Assert.NotNull(actions[1].MetaData);
            Assert.Contains(actions, x => x.Add != null);
            Assert.True(RetentionProperties.Matches(actions[1].MetaData!.Configuration, Published));

            // No metadata-only commit follows
            await RunCheckpoints(stream, 3);
            Assert.False(await storage.Exists(CommitPath("test", 2)));
            await AssertReaderResult(testName, storage, "test", stream.Users.Select(x => new { x.UserKey, x.FirstName }));
        }

        [Fact]
        public async Task PropertiesCommitStagedOnce()
        {
            var testName = nameof(PropertiesCommitStagedOnce);
            var inner = await ExistingTable(testName);
            var storage = new HookFileStorage(inner);
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            await WaitForVersionCheckpointing(inner, "test", stream, 1);

            // Published once, the applied header ends the pending state
            await RunCheckpoints(stream, 5);
            Assert.False(await inner.Exists(CommitPath("test", 2)));
            Assert.Single(storage.Requests, x => x.StartsWith("OpenWrite ") && x.Contains("/_delta_log/.0000"));

            // A restart reads the published properties from the log, its next commit carries none
            await stream.Crash();
            stream.Generate(1);
            await WaitForVersion(inner, "test", stream, 2);
            Assert.DoesNotContain(await ReadCommitActions(storage, "test", 2), x => x.MetaData != null);
            Assert.Single(storage.Requests, x => x.StartsWith("OpenWrite ") && x.Contains("/_delta_log/.00000000000000000001.json."));
        }

        [Fact]
        public async Task UnsetLeavesProperties()
        {
            var testName = nameof(UnsetLeavesProperties);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using (var retained = new DeltaLakeSinkStream(testName, storage, Retained))
            {
                retained.Generate(5);
                await retained.StartStream(UserInsert);
                await WaitForVersion(storage, "test", retained, 0);
            }

            // A replacement sink without retention
            await using var stream = new DeltaLakeSinkStream(testName + "_unset", storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);
            await RunCheckpoints(stream, 3);

            Assert.DoesNotContain(await ReadCommitActions(storage, "test", 1), x => x.MetaData != null);
            Assert.False(await storage.Exists(CommitPath("test", 2)));
            Assert.True(RetentionProperties.Matches(await TableConfiguration(storage), Published));
            Assert.Null(stream.Sinks.Single().WorkerSettingsForTests!.Retention);
        }

        [Fact]
        public async Task AnchorRequestBypassesIdleReturn()
        {
            var testName = nameof(AnchorRequestBypassesIdleReturn);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await RunCheckpoints(stream, 2);
            Assert.False(await storage.Exists(CommitPath("test", 1)));

            var worker = Assert.Single(stream.Maintenance.Workers);
            var generation = worker.RequestAnchor();
            await WaitForVersionCheckpointing(storage, "test", stream, 1);

            // The anchor carries the unchanged metaData, which makes it a commit
            var actions = await ReadCommitActions(storage, "test", 1);
            Assert.Equal(2, actions.Count);
            Assert.NotNull(actions[0].CommitInfo);
            var created = Assert.Single(await ReadCommitActions(storage, "test", 0), x => x.MetaData != null).MetaData!;
            Assert.Equal(created.Configuration!.OrderBy(x => x.Key), actions[1].MetaData!.Configuration!.OrderBy(x => x.Key));
            Assert.Equal(created.Id, actions[1].MetaData!.Id);
            Assert.Equal(0, worker.AnchorRequest);
            Assert.Equal(1, worker.AnchorStagedVersion);
            Assert.True(generation > 0);

            // One request, one commit
            await RunCheckpoints(stream, 3);
            Assert.False(await storage.Exists(CommitPath("test", 2)));
        }

        [Fact]
        public async Task AnchorAcknowledgedOnlyAfterStaging()
        {
            var testName = nameof(AnchorAcknowledgedOnlyAfterStaging);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            await RunCheckpoints(stream, 1);
            var sink = stream.Sinks.Single();
            var worker = Assert.Single(stream.Maintenance.Workers);

            // What the worker sees while the stage is written; attempts fail until the test allows them
            var fail = 1;
            var seenAtStage = new ConcurrentQueue<long?>();
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.Contains("/_delta_log/.00000000000000000001.json."))
                {
                    seenAtStage.Enqueue(worker.AnchorStagedVersion);
                    if (Volatile.Read(ref fail) == 1)
                    {
                        throw new IOException("stage write fault");
                    }
                }
                return Task.CompletedTask;
            };
            var generation = worker.RequestAnchor();

            // Failed stages are contained and acknowledge nothing
            await WaitForFailures(stream, sink, 2);
            Assert.Equal(generation, worker.AnchorRequest);
            Assert.Null(worker.AnchorStagedVersion);
            Assert.False(await storage.Inner.Exists(CommitPath("test", 1)));

            // A later checkpoint stages it and acknowledges after the stage exists
            Volatile.Write(ref fail, 0);
            await WaitForVersionCheckpointing(storage.Inner, "test", stream, 1);
            Assert.All(seenAtStage, x => Assert.Null(x));
            Assert.True(seenAtStage.Count >= 3);
            Assert.Equal(1, worker.AnchorStagedVersion);
            Assert.Equal(0, worker.AnchorRequest);
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task DataCommitCarriesTheAnchor()
        {
            var testName = nameof(DataCommitCarriesTheAnchor);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            await RunCheckpoints(stream, 1);
            var worker = Assert.Single(stream.Maintenance.Workers);

            // Version 1 is staged only by a commit that wrote a data file
            var dataWritten = 0;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.Contains("/part-"))
                {
                    Volatile.Write(ref dataWritten, 1);
                }
                else if (verb == "OpenWrite" && path.Full.Contains("/_delta_log/.00000000000000000001.json.") && Volatile.Read(ref dataWritten) == 0)
                {
                    throw new IOException("idle stage refused");
                }
                return Task.CompletedTask;
            };
            var generation = worker.RequestAnchor();
            stream.Generate(5);
            await WaitForVersion(storage.Inner, "test", stream, 1);

            // commitInfo first, then the unchanged metaData, then the rows
            var actions = await ReadCommitActions(storage.Inner, "test", 1);
            Assert.Equal("WRITE", actions[0].CommitInfo!.Operation);
            Assert.Same(Assert.Single(actions, x => x.MetaData != null), actions[1]);
            Assert.Contains(actions, x => x.Add != null);
            var created = Assert.Single(await ReadCommitActions(storage.Inner, "test", 0), x => x.MetaData != null).MetaData!;
            Assert.Equal(created.Configuration!.OrderBy(x => x.Key), actions[1].MetaData!.Configuration!.OrderBy(x => x.Key));
            Assert.Equal(created.Id, actions[1].MetaData!.Id);
            Assert.True(generation > 0);
            Assert.Equal(1, worker.AnchorStagedVersion);
            Assert.Equal(0, worker.AnchorRequest);

            // No anchor-only commit follows
            await RunCheckpoints(stream, 3);
            Assert.False(await storage.Inner.Exists(CommitPath("test", 2)));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task IdlePropertiesFailureNeverReachesEngine()
        {
            var testName = nameof(IdlePropertiesFailureNeverReachesEngine);
            var inner = await ExistingTable(testName);
            var storage = new HookFileStorage(inner);
            // Every stage write fails, then every log listing too
            var faults = 1;
            var listingFaults = 0;
            storage.Before = (verb, path) =>
            {
                var mode = Volatile.Read(ref faults);
                if (mode >= 1 && verb == "OpenWrite" && path.Full.Contains("/_delta_log/."))
                {
                    throw new IOException("stage write fault");
                }
                if (mode == 2 && verb == "Ls" && path.Full.Contains("/_delta_log"))
                {
                    Interlocked.Increment(ref listingFaults);
                    throw new IOException("listing fault");
                }
                return Task.CompletedTask;
            };
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            var sink = await WaitForSink(stream);

            await WaitForFailures(stream, sink, 2);
            Volatile.Write(ref faults, 2);
            var failures = sink.MetadataCommitFailures;
            await WaitForFailures(stream, sink, failures + 2, () => Volatile.Read(ref listingFaults) >= 2);
            Assert.False(await inner.Exists(CommitPath("test", 1)));
            Assert.Equal(0, stream.FailureNotificationCount);

            // Retried at the next checkpoint once the storage recovers
            Volatile.Write(ref faults, 0);
            await WaitForVersionCheckpointing(inner, "test", stream, 1);
            Assert.True(RetentionProperties.Matches(await TableConfiguration(storage), Published));
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task IdleRetryOfAnUnreadyCatalogNeverReachesEngine()
        {
            var testName = nameof(IdleRetryOfAnUnreadyCatalogNeverReachesEngine);
            var inner = await ExistingTable(testName);
            var storage = new HookFileStorage(inner);
            // Every stage write fails, then every read of a commit file instead
            var faults = 1;
            var readFaults = 0;
            storage.Before = (verb, path) =>
            {
                var mode = Volatile.Read(ref faults);
                if (mode == 1 && verb == "OpenWrite" && path.Full.Contains("/_delta_log/."))
                {
                    throw new IOException("stage write fault");
                }
                if (mode == 2 && (verb == "OpenRead" || verb == "ReadText") && path.Full.Contains("/_delta_log/0"))
                {
                    Interlocked.Increment(ref readFaults);
                    throw new IOException("log read fault");
                }
                return Task.CompletedTask;
            };
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            var sink = await WaitForSink(stream);
            await WaitForFailures(stream, sink, 1);
            // No checkpoint in flight across the head move
            await stream.WaitForCheckpointsToSettle();

            // Another writer moves the head; the catalog's read of it fails and leaves the catalog unready
            await WriteCommit(inner, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
            Volatile.Write(ref faults, 2);
            var failures = sink.MetadataCommitFailures;
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (stream.FailureNotificationCount == 0 && (sink.MetadataCommitFailures < failures + 3 || Volatile.Read(ref readFaults) < 3))
            {
                Assert.True(DateTime.UtcNow < deadline, $"{sink.MetadataCommitFailures - failures} failures contained, {readFaults} reads failed");
                await stream.TriggerCheckpoint();
                await stream.SchedulerTick();
                await Task.Delay(1);
            }
            Assert.True(stream.FailureNotificationCount == 0, $"the engine saw {stream.FailureNotificationCount} failures after {sink.MetadataCommitFailures - failures} contained ones");
            Assert.False(await inner.Exists(CommitPath("test", 2)));

            // Retried at the next checkpoint once the storage recovers
            Volatile.Write(ref faults, 0);
            await WaitForVersionCheckpointing(inner, "test", stream, 2);
            Assert.True(RetentionProperties.Matches(await TableConfiguration(inner), Published));
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        // Stage writes fail in mode 1, reads of log files in mode 2
        private static HookFileStorage Faulting(IFileStorage inner, Func<int> mode, Action onRead)
        {
            var storage = new HookFileStorage(inner);
            storage.Before = (verb, path) =>
            {
                var current = mode();
                if (current == 1 && verb == "OpenWrite" && path.Full.Contains("/_delta_log/."))
                {
                    throw new IOException("stage write fault");
                }
                if (current == 2 && (verb == "OpenRead" || verb == "ReadText") && path.Full.Contains("/_delta_log/0"))
                {
                    onRead();
                    throw new IOException("log read fault");
                }
                return Task.CompletedTask;
            };
            return storage;
        }

        // Checkpoints until the engine saw a failure after the given count, or the sink contained one after its count
        private static Task WaitForEngineOrContained(DeltaLakeSinkStream stream, DeltaLakeSink sink, long engine, long contained)
        {
            return TickUntil(stream, () => Task.FromResult(stream.FailureNotificationCount > engine || sink.MetadataCommitFailures > contained), () => "neither the engine nor the sink saw a failure");
        }

        // Checkpoints until the condition holds; once the engine failed, the harness rethrows that failure at every tick
        private static async Task TickUntil(DeltaLakeSinkStream stream, Func<Task<bool>> condition, Func<string> timedOut)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!await condition())
            {
                Assert.True(DateTime.UtcNow < deadline, timedOut());
                try
                {
                    await stream.TriggerCheckpoint();
                    await stream.SchedulerTick();
                }
                catch (AggregateException e) when (e.InnerExceptions.Any(x => x is IOException))
                {
                }
                await Task.Delay(1);
            }
        }

        [Fact]
        public async Task RestartForgetsAnEarlierRetentionCommitFailure()
        {
            var testName = nameof(RestartForgetsAnEarlierRetentionCommitFailure);
            var inner = await ExistingTable(testName);
            var faults = 1;
            var readFaults = 0;
            var storage = Faulting(inner, () => Volatile.Read(ref faults), () => Interlocked.Increment(ref readFaults));
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            var sink = await WaitForSink(stream);
            await WaitForFailures(stream, sink, 1);
            // Published once the storage recovers, then nothing is pending
            Volatile.Write(ref faults, 0);
            await WaitForVersionCheckpointing(inner, "test", stream, 1);
            await RunCheckpoints(stream, 2);

            // After a restart the first checkpoint's read of the log fails
            var contained = sink.MetadataCommitFailures;
            Volatile.Write(ref faults, 2);
            await stream.Crash();
            var engine = stream.FailureNotificationCount;
            await WaitForEngineOrContained(stream, sink, engine, contained);

            // As any failed check: the engine sees it, and it is no retention commit failure
            Assert.True(Volatile.Read(ref readFaults) > 0);
            Assert.Equal(contained, sink.MetadataCommitFailures);
            Assert.True(stream.FailureNotificationCount > engine);
            Volatile.Write(ref faults, 0);
        }

        [Fact]
        public async Task FailedCheckOnAFirstStartReachesTheEngine()
        {
            var testName = nameof(FailedCheckOnAFirstStartReachesTheEngine);
            var inner = await ExistingTable(testName, new Dictionary<string, string>() { [RetentionProperties.LogKey] = Published, [RetentionProperties.DeletedFileKey] = Published });
            var faults = 2;
            var readFaults = 0;
            var storage = Faulting(inner, () => Volatile.Read(ref faults), () => Interlocked.Increment(ref readFaults));
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            var sink = await WaitForSink(stream);

            // Nothing waits to be staged and no retention commit failed before: the failed read of the log is the engine's
            await WaitForEngineOrContained(stream, sink, 0, 0);
            Assert.True(Volatile.Read(ref readFaults) > 0);
            Assert.Equal(0, sink.MetadataCommitFailures);
            Assert.True(stream.FailureNotificationCount > 0);
            Volatile.Write(ref faults, 0);
        }

        [Fact]
        public async Task FailedCheckWithRowsReachesTheEngineAndKeepsThem()
        {
            var testName = nameof(FailedCheckWithRowsReachesTheEngineAndKeepsThem);
            var inner = await ExistingTable(testName);
            var faults = 1;
            var readFaults = 0;
            var storage = Faulting(inner, () => Volatile.Read(ref faults), () => Interlocked.Increment(ref readFaults));
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            var sink = await WaitForSink(stream);
            await WaitForFailures(stream, sink, 1);
            // No checkpoint in flight across the head move
            await stream.WaitForCheckpointsToSettle();

            // Another writer moves the head; the catalog's read of it fails and leaves the catalog unready, contained while no rows wait
            await WriteCommit(inner, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
            Volatile.Write(ref faults, 2);
            await WaitForFailures(stream, sink, sink.MetadataCommitFailures + 2, () => Volatile.Read(ref readFaults) >= 2);
            Assert.Equal(0, stream.FailureNotificationCount);

            // Rows arrive while the log cannot be read: their checkpoint fails and keeps them
            stream.Generate(5);
            await TickUntil(stream, () => Task.FromResult(stream.FailureNotificationCount > 0), () => $"the engine saw no failure after {sink.MetadataCommitFailures} contained ones");
            Assert.False(await inner.Exists(CommitPath("test", 2)));

            // Every row is published once the log can be read again
            Volatile.Write(ref faults, 0);
            await TickUntil(stream, () => inner.Exists(CommitPath("test", 2)), () => "the rows were not published after the log recovered");
            await AssertReaderResult(testName, inner, "test", stream.Users.Select(x => new { x.UserKey, x.FirstName }));
        }

        [Fact]
        public async Task ContainedFailureNeverRestoresARetractedRow()
        {
            var testName = nameof(ContainedFailureNeverRestoresARetractedRow);
            // Its own table name for the shared tree hook
            const string table = "retracted_row_contained";
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            var treeCommits = 0;
            using var hook = DeltaLakeSinkHaltTests.HookTreeCommits(name =>
            {
                if (name == table)
                {
                    Interlocked.Increment(ref treeCommits);
                }
            });
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert.Replace("test", table));
            await WaitForVersion(storage, table, stream, 0);

            // Another writer takes version 1 before the sink publishes it; once armed, the next retry finds it gone
            var foreignWritten = 0;
            var armed = 0;
            var stageFaults = 0;
            User? retracted = null;
            storage.Before = async (verb, path) =>
            {
                if (Volatile.Read(ref stageFaults) == 1 && verb == "OpenWrite" && path.Full.Contains("/_delta_log/."))
                {
                    throw new IOException("stage write fault");
                }
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath(table, 1)))
                {
                    if (Interlocked.Exchange(ref foreignWritten, 1) == 0)
                    {
                        await WriteCommit(inner, table, 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                    }
                    else if (Interlocked.CompareExchange(ref armed, 2, 1) == 1)
                    {
                        // The row the halted checkpoints kept is retracted, and the next idle anchor commit fails to stage
                        await inner.Rm(CommitPath(table, 1));
                        stream.DeleteUser(retracted!);
                        stream.Sinks.Single().StartedWorkerForTests!.RequestAnchor();
                        Volatile.Write(ref stageFaults, 1);
                    }
                }
            };
            stream.Generate(5);
            await TickUntil(stream, () => Task.FromResult(logs.Errors.Count >= 1), () => "the sink did not halt");
            stream.Generate(1);
            retracted = stream.Users.Last();
            var sink = stream.Sinks.Single();
            string State() => $"armed {Volatile.Read(ref armed)}, {Volatile.Read(ref treeCommits)} tree commits, {sink.MetadataCommitFailures} contained failures, {stream.FailureNotificationCount} engine failures, errors: {string.Join(" | ", logs.Errors)}";

            // The row waits in the temporary tree, committed by a halted checkpoint
            await TickUntil(stream, () => Task.FromResult(Volatile.Read(ref treeCommits) >= 1), State);
            await stream.WaitForCheckpointsToSettle();

            // One halted checkpoint whose retry publishes; only the retraction's own checkpoint follows it
            Volatile.Write(ref armed, 1);
            await stream.TriggerCheckpoint();
            await TickOnly(stream, () => Volatile.Read(ref armed) == 2, State);
            await TickOnly(stream, () => sink.MetadataCommitFailures >= 1, State);

            // A second contained failure: the checkpoint of the first one completed
            await TickUntil(stream, () => Task.FromResult(sink.MetadataCommitFailures >= 2), State);
            Assert.Equal(0, stream.FailureNotificationCount);

            // Restored at that checkpoint, the next data commit writes only the new row
            await stream.Crash();
            Volatile.Write(ref stageFaults, 0);
            stream.Generate(1);
            await TickUntil(stream, () => inner.Exists(CommitPath(table, 2)), () => "nothing was published after the restart");
            await SettlePublications(stream);
            await AssertReaderResult(testName, inner, table, stream.Users.Select(x => new { x.UserKey, x.FirstName }));
        }

        [Fact]
        public async Task FailedTreeCommitWithoutRowsReachesTheEngine()
        {
            var testName = nameof(FailedTreeCommitWithoutRowsReachesTheEngine);
            // Its own table name for the shared tree hook
            const string table = "tree_commit_uncontained";
            var inner = Files.Of.InternalMemory($"./{testName}");
            var storage = new HookFileStorage(inner);
            var logs = new TestLogCollector();
            var treeFaults = 0;
            var treeCommits = 0;
            using var hook = DeltaLakeSinkHaltTests.HookTreeCommits(name =>
            {
                if (name == table)
                {
                    Interlocked.Increment(ref treeCommits);
                    if (Volatile.Read(ref treeFaults) == 1)
                    {
                        throw new IOException("tree commit fault");
                    }
                }
            });
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.AddLoggerProvider(logs);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert.Replace("test", table));
            await WaitForVersion(storage, table, stream, 0);

            // The retracted-row scenario, with the emptied tree's commit failing instead of the anchor's stage
            var foreignWritten = 0;
            var armed = 0;
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(CommitPath(table, 1)))
                {
                    if (Interlocked.Exchange(ref foreignWritten, 1) == 0)
                    {
                        await WriteCommit(inner, table, 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });
                    }
                    else if (Interlocked.CompareExchange(ref armed, 2, 1) == 1)
                    {
                        await inner.Rm(CommitPath(table, 1));
                        stream.Sinks.Single().StartedWorkerForTests!.RequestAnchor();
                        Volatile.Write(ref treeFaults, 1);
                    }
                }
            };
            stream.Generate(5);
            await TickUntil(stream, () => Task.FromResult(logs.Errors.Count >= 1), () => "the sink did not halt");
            stream.Generate(1);
            var retracted = stream.Users.Last();
            var sink = stream.Sinks.Single();
            string State() => $"armed {Volatile.Read(ref armed)}, {Volatile.Read(ref treeCommits)} tree commits, {sink.MetadataCommitFailures} contained failures, {stream.FailureNotificationCount} engine failures, errors: {string.Join(" | ", logs.Errors)}";
            // The row waits in the temporary tree, committed by a halted checkpoint
            await TickUntil(stream, () => Task.FromResult(Volatile.Read(ref treeCommits) >= 1), State);

            // Retracted while halted, committed by its own checkpoint
            await stream.WaitForCheckpointsToSettle();
            var settled = Volatile.Read(ref treeCommits);
            stream.DeleteUser(retracted);
            await TickOnly(stream, () => Volatile.Read(ref treeCommits) > settled, State);

            // The next publish retry arms the fault
            Volatile.Write(ref armed, 1);
            await TickUntil(stream, () => Task.FromResult(Volatile.Read(ref armed) == 2), State);
            await TickUntil(stream, () => Task.FromResult(stream.FailureNotificationCount > 0 || sink.MetadataCommitFailures > 0), State);
            Assert.Equal(0, sink.MetadataCommitFailures);
            Assert.True(stream.FailureNotificationCount > 0);
            // Nothing staged after the publish
            Assert.DoesNotContain(await inner.Ls($"/{table}/_delta_log/"), x => x.Name.StartsWith($".{2:D20}.json."));
            Volatile.Write(ref treeFaults, 0);
        }

        // Scheduler ticks only, no checkpoint beyond the ones the stream schedules itself
        private static async Task TickOnly(DeltaLakeSinkStream stream, Func<bool> condition, Func<string> timedOut)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!condition())
            {
                Assert.True(DateTime.UtcNow < deadline, timedOut());
                try
                {
                    await stream.SchedulerTick();
                }
                catch (AggregateException e) when (e.InnerExceptions.Any(x => x is IOException))
                {
                }
                await Task.Delay(5);
            }
        }

        private static async Task<DeltaLakeSink> WaitForSink(DeltaLakeSinkStream stream)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (stream.Sinks.Count == 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "no sink was created");
                await Task.Delay(1);
            }
            return stream.Sinks.Single();
        }

        // Checkpoints until the sink counted the failures
        private static async Task WaitForFailures(DeltaLakeSinkStream stream, DeltaLakeSink sink, long count, Func<bool>? also = null)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (sink.MetadataCommitFailures < count || also?.Invoke() == false)
            {
                Assert.True(DateTime.UtcNow < deadline, $"{sink.MetadataCommitFailures} of {count} failures counted");
                await stream.TriggerCheckpoint();
                await stream.SchedulerTick();
                await Task.Delay(1);
            }
        }

        [Fact]
        public async Task PartitionedIdleTableGetsMetadataOnlyCommit()
        {
            var testName = nameof(PartitionedIdleTableGetsMetadataOnlyCommit);
            var storage = await ExistingTable(testName, partitionColumns: new List<string>() { "name" });
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            await WaitForVersionCheckpointing(storage, "test", stream, 1);

            var actions = await ReadCommitActions(storage, "test", 1);
            Assert.Equal(2, actions.Count);
            Assert.Equal(new[] { "name" }, actions[1].MetaData!.PartitionColumns);
            Assert.True(RetentionProperties.Matches(actions[1].MetaData!.Configuration, Published));
            Assert.Equal(0, stream.Sinks.Single().MetadataCommitFailures);
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task IdleCommitIgnoresARequiredColumnTheQueryDoesNotWrite()
        {
            var testName = nameof(IdleCommitIgnoresARequiredColumnTheQueryDoesNotWrite);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var schema = UserSchema.Replace("\"fields\":[", "\"fields\":[{\"name\":\"required_extra\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},", StringComparison.Ordinal);
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(schema));
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            var sink = await WaitForSink(stream);
            await TickUntil(stream, async () => sink.MetadataCommitFailures > 0 || await storage.Exists(CommitPath("test", 1)), () => "no properties commit was attempted");

            // Staged without a row writer, the schema string as read
            Assert.Equal(0, sink.MetadataCommitFailures);
            var actions = await ReadCommitActions(storage, "test", 1);
            Assert.Equal(2, actions.Count);
            Assert.Equal(schema, actions[1].MetaData!.SchemaString);
            Assert.True(RetentionProperties.Matches(actions[1].MetaData!.Configuration, Published));
            Assert.Equal(0, stream.FailureNotificationCount);
        }

        [Fact]
        public async Task StagedSweepTemporaryAgeFollowsRetention()
        {
            var testName = nameof(StagedSweepTemporaryAgeFollowsRetention);

            // A sink temporary one and a half days older than the newest commit, after one sweep
            async Task<bool> TemporaryKept(TimeSpan? retention)
            {
                var directory = StoreFixtures.TempDirectory(testName);
                using var storage = Files.Of.LocalDisk(directory);
                await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.HistoryRetention = retention);
                stream.Generate(5);
                await stream.StartStream(UserInsert);
                await WaitForVersion(storage, "test", stream, 0);
                var temporary = Path.Combine(directory, "test", "_delta_log", Temporary);
                File.WriteAllText(temporary, "x");
                File.SetLastWriteTimeUtc(temporary, DateTime.UtcNow - TimeSpan.FromDays(1.5));
                await stream.Maintenance.TickAndSettleAsync(0);
                Assert.NotNull(stream.Maintenance.Workers.Single().SweepDueAt);
                return File.Exists(temporary);
            }

            Assert.True(await TemporaryKept(TimeSpan.FromDays(2)));
            Assert.False(await TemporaryKept(null));
        }
    }
}
