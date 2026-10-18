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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class UnvouchedWrapperDiscardTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "uw-" + ScratchOwners.ShortId());

        public UnvouchedWrapperDiscardTests()
        {
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        [Fact]
        public async Task UnvouchedWrapperStillDeletesDiscardedChangeData()
        {
            var testName = nameof(UnvouchedWrapperStillDeletesDiscardedChangeData);
            // A user's own wrapper over a store whose Stat reports sizes: not a pinned Stowage type and not vouched for
            using var local = Files.Of.LocalDisk(_directory);
            var storage = new HookFileStorage(local);
            var version = await WriteInsertOnlyVersion(testName, storage);

            // Deleted through the storage that wrote them
            Assert.NotEmpty(version.Written);
            Assert.Empty(version.Left);
            Assert.Equal((version.Written.Count, 0L), (version.Deleted, version.Kept));
            // Each by its exact key after a Stat showed a file
            Assert.All(version.Written, x => Assert.Contains("Stat " + x, storage.Requests));
            Assert.All(version.Written, x => Assert.Contains("Rm " + x, storage.Requests));
        }

        [Fact]
        public async Task UnvouchedWrapperWithoutSizesKeepsDiscardedChangeData()
        {
            var testName = nameof(UnvouchedWrapperWithoutSizesKeepsDiscardedChangeData);
            // In-memory Stat reports no size, so nothing shows a file
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var version = await WriteInsertOnlyVersion(testName, storage);

            Assert.NotEmpty(version.Written);
            Assert.Equal(version.Written.OrderBy(x => x, StringComparer.Ordinal), version.Left.OrderBy(x => x, StringComparer.Ordinal));
            Assert.Equal((0L, version.Written.Count), (version.Deleted, version.Kept));
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("Rm /test/_change_data/", StringComparison.Ordinal));
        }

        // What an insert-only version 1, whose change data files are all discarded, wrote and left, and the deletes and keeps the sink counted for it
        private sealed record InsertOnlyVersion(List<string> Written, List<string> Left, long Deleted, long Kept);

        private static async Task<InsertOnlyVersion> WriteInsertOnlyVersion(string testName, HookFileStorage storage)
        {
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 500;
                options.SinkCreatedForTests = created => sink = created;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            Assert.Equal(DiscardMode.Inline, sink!.DiscardModeForTests);

            var before = (await storage.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder).Select(x => x.Path.Full).ToHashSet();
            var deletedBefore = sink.DiscardsForTests.Deleted;
            var keptBefore = sink.DiscardsForTests.Kept;
            var written = new List<string>();
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.Contains("/_change_data/"))
                {
                    lock (written)
                    {
                        written.Add(path.Full);
                    }
                }
                return Task.CompletedTask;
            };
            storage.ClearRequests();
            stream.Generate(1000);
            await WaitForVersion(storage.Inner, "test", stream, 1);

            var left = (await storage.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder && !before.Contains(x.Path.Full)).Select(x => x.Path.Full).ToList();
            Assert.Equal(0, stream.FailureNotificationCount);
            return new InsertOnlyVersion(written.Distinct().ToList(), left, sink.DiscardsForTests.Deleted - deletedBefore, sink.DiscardsForTests.Kept - keptBefore);
        }

        [Fact]
        public async Task UnvouchedStackDeletesOnlyQualifiedFilesOfPositiveSize()
        {
            var objects = new ScriptedStorage();
            foreach (var name in new[] { "own.parquet", "x./cdc.parquet", "cdc 1.parquet", "folder.parquet" })
            {
                objects.Put("/t/_change_data/" + name);
            }
            objects.Put("/t/_change_data/empty.parquet", 0);
            objects.Put("/t/_delta_log/x.parquet");
            objects.StatAnswer = (path, entry) => path.Name == "folder.parquet" ? new IOEntry(path.Full + "/") : entry;
            var configuration = CleanupConfiguration.Validate(objects, "t", false, CleanupOptIns.Off);
            Assert.Equal(StoreKind.Unknown, configuration.Store.Store.Kind);
            Assert.False(configuration.Store.Eligible);
            var executor = new DeletionExecutor(configuration, objects);

            // Without the assertion a queued drain's target stays unsupported
            Assert.Equal(KeepReason.StoreUnsupported, executor.TryTarget("_change_data/own.parquet", TargetKind.Payload, out _));
            Assert.Equal(new DeleteOutcome(DeleteStatus.Deleted), await executor.DeleteOwnAsync("_change_data/own.parquet", default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await executor.DeleteOwnAsync("_change_data/empty.parquet", default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Directory), await executor.DeleteOwnAsync("_change_data/folder.parquet", default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await executor.DeleteOwnAsync("_change_data/gone.parquet", default));
            // Names a stack may trim or send unencoded, and payload inside a log, are kept without a request
            var requests = objects.Requests.Count;
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await executor.DeleteOwnAsync("_change_data/x./cdc.parquet", default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await executor.DeleteOwnAsync("_change_data/cdc 1.parquet", default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Misplaced), await executor.DeleteOwnAsync("_delta_log/x.parquet", default));
            Assert.Equal(requests, objects.Requests.Count);
            Assert.Equal(new[] { "Rm /t/_change_data/own.parquet" }, objects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));
            Assert.Equal((1L, 6L, 0L), (executor.Tally.Deleted, executor.Tally.Kept, executor.Tally.Absent));

            // A table path outside the qualified characters, a store whose deletes are not exact, or another storage instance keep every name
            var unsafeTable = new ScriptedStorage();
            unsafeTable.Put("/t+1/_change_data/own.parquet");
            var unsafeExecutor = new DeletionExecutor(CleanupConfiguration.Validate(unsafeTable, "t+1", false, CleanupOptIns.Off), unsafeTable);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StoreUnsupported), await unsafeExecutor.DeleteOwnAsync("_change_data/own.parquet", default));
            var inexact = new ScriptedStorage();
            inexact.Put("/t/_change_data/own.parquet");
            var inexactStore = new StoreDescriptor { Kind = StoreKind.Unknown, QualifiedDomain = true, NeedsAssertion = true, ExactDeletes = false, Detail = "inexact" };
            var inexactExecutor = new DeletionExecutor(CleanupConfiguration.Validate(CleanupStoreContract.Create(inexact, inexactStore, "t", false), CleanupOptIns.Off), inexact);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StoreUnsupported), await inexactExecutor.DeleteOwnAsync("_change_data/own.parquet", default));
            var replaced = new ScriptedStorage();
            replaced.Put("/t/_change_data/own.parquet");
            var replacedExecutor = new DeletionExecutor(configuration, replaced);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StorageChanged), await replacedExecutor.DeleteOwnAsync("_change_data/own.parquet", default));
            Assert.Empty(unsafeTable.Requests.Concat(inexact.Requests).Concat(replaced.Requests));
        }

        [Fact]
        public async Task UnvouchedOwnOutputWhoseStatShowsNothingIsKeptNotCountedDeleted()
        {
            // The attempt's own output was just written, so a Stat that shows nothing is the stack's word, which nobody vouched for
            var objects = new ScriptedStorage();
            var executor = new DeletionExecutor(CleanupConfiguration.Validate(objects, "t", false, CleanupOptIns.Off), objects);
            var counters = new DiscardCounters();
            var result = await InlineDiscards.DeleteAsync(executor, new[] { "_change_data/own.parquet" }, counters);
            Assert.Equal((0, 0, 1, 0), (result.Deleted, result.Absent, result.Kept, result.Failed));
            Assert.Equal(KeepReason.UnknownSize, result.FirstKept);
            Assert.Equal((0L, 0L, 1L), (counters.Deleted, counters.Absent, counters.Kept));
            Assert.Equal(new[] { "Stat /t/_change_data/own.parquet" }, objects.Requests);

            // Vouched for, a Stat that shows nothing is a missing object
            var vouched = new ScriptedStorage();
            var asserted = new DeletionExecutor(CleanupConfiguration.Validate(vouched, "t", true, CleanupOptIns.Off), vouched);
            Assert.Equal(new DeleteOutcome(DeleteStatus.Absent), await asserted.DeleteOwnAsync("_change_data/own.parquet", default));
            Assert.DoesNotContain(vouched.Requests, x => x.StartsWith("Rm ", StringComparison.Ordinal));
        }
    }
}
