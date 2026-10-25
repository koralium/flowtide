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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeCheckpointBuildTests
    {
        private static bool IsCommit(IOPath path) => DeltaTransactionReader.TryGetCommitVersion(path.Name, out _);

        private static string CommitName(long version) => $"{version:D20}.json";

        // A build without publication that returns its failure
        private async Task<(BuildResult? Result, Exception? Failure)> BuildReadingAhead(IFileStorage storage, long version, int readAhead, JobMemory memory, TransferAllowance transfer,
            TimeProvider? clock = null, DateTimeOffset? deadline = null, CancellationToken cancellationToken = default)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            try
            {
                using var scratch = owner.CreateJob();
                var context = new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = memory,
                    Clock = clock ?? new FakeTimeProvider(s_now),
                    Publish = false,
                    ReadAhead = readAhead,
                    Transfer = transfer
                };
                return (await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = version, Deadline = deadline }, cancellationToken), null);
            }
            catch (Exception e)
            {
                return (null, e);
            }
            finally
            {
                _owners.Release(owner);
            }
        }

        // Every commit open the build entered was returned and disposed, and nothing stays charged
        private static void AssertReleased(TrackedHookFileStorage storage, JobMemory memory, TransferAllowance transfer)
        {
            Assert.Equal(storage.Returned, storage.Disposed);
            Assert.Equal(0, transfer.Charged);
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(8)]
        [InlineData(20)]
        public async Task LongReplayTakesOneStoreRoundTripPerWindow(int readAhead)
        {
            var storage = TrackedHookFileStorage.Over(Memory(nameof(LongReplayTakesOneStoreRoundTripPerWindow)));
            await WriteHistory(storage, "t", 4, 60, 40);
            var rounds = new OpenRounds();
            storage.Before = rounds.Hook(IsCommit);

            var work = Build(storage, "t", 59, RootPolicy.None, readAhead: readAhead, transfer: new TransferAllowance());
            var trips = await rounds.DriveAsync(work, readAhead, 60);
            var built = await work;
            storage.Before = null;

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal((60 + readAhead - 1) / readAhead, trips);
            Assert.Equal(readAhead, storage.MaxOutstanding);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 59, s_now), built.Lines);
            _output.WriteLine($"Read ahead {readAhead}: {trips} store round trips for 60 commits, {60 / (trips * 0.03):0} versions a second at 30 ms a read");
        }

        [Theory]
        [InlineData(1, false)]
        [InlineData(2, false)]
        [InlineData(3, false)]
        [InlineData(1, true)]
        [InlineData(2, true)]
        [InlineData(3, true)]
        public async Task ReadAheadBuildMatchesTheSequentialBuild(int seed, bool spill)
        {
            var arenaCap = spill ? 4096 : TailReducer.DefaultArenaCap;
            async Task<(Built Built, TrackedHookFileStorage Storage)> BuildWith(int readAhead)
            {
                var storage = TrackedHookFileStorage.Over(Memory(nameof(ReadAheadBuildMatchesTheSequentialBuild)));
                await WriteHistory(storage, "t", seed, 40, 40);
                var rounds = new OpenRounds();
                storage.Before = rounds.Hook(IsCommit);
                var work = Build(storage, "t", 39, RootPolicy.None, arenaCap, readAhead: readAhead, transfer: new TransferAllowance());
                await rounds.PumpAsync(work);
                return (await work, storage);
            }

            var (ahead, aheadStorage) = await BuildWith(16);
            var (sequential, _) = await BuildWith(1);

            Assert.Equal(BuildStatus.Completed, ahead.Result.Status);
            Assert.Equal(spill ? BuildMode.Sort : BuildMode.Probe, ahead.Result.Mode);
            Assert.Equal(sequential.Result.Status, ahead.Result.Status);
            Assert.Equal(sequential.Lines, ahead.Lines);
            Assert.Equal(sequential.Result.Checkpoint!.Sha256, ahead.Result.Checkpoint!.Sha256);
            Assert.Equal(sequential.Result.Checkpoint.Actions, ahead.Result.Checkpoint.Actions);
            Assert.Equal(sequential.Result.Checkpoint.AddFiles, ahead.Result.Checkpoint.AddFiles);
            Assert.Equal(sequential.Result.Mode, ahead.Result.Mode);
            Assert.Equal(sequential.Result.TransferBytes, ahead.Result.TransferBytes);
            Assert.Equal(sequential.Result.SkippedLines, ahead.Result.SkippedLines);
            Assert.Equal(sequential.Result.ProtocolCommits, ahead.Result.ProtocolCommits);
            Assert.Equal(sequential.Marker, ahead.Marker);
            Assert.True(aheadStorage.MaxOutstanding > 1, $"{aheadStorage.MaxOutstanding} commit opens overlapped");
        }

        [Theory]
        [InlineData("missing")]
        [InlineData("io")]
        public async Task MiddleVersionFailureSurfacesAtItsTurn(string fault)
        {
            async Task<(Exception? Failure, TrackedHookFileStorage Storage, JobMemory Memory, TransferAllowance Transfer)> Run(int readAhead)
            {
                var storage = TrackedHookFileStorage.Over(Memory(nameof(MiddleVersionFailureSurfacesAtItsTurn)));
                await DeltaMaintenanceWorkerTests.Commits(storage, 0, 29);
                // Commit 12 goes after the listing
                var listed = false;
                storage.Before = (verb, path) =>
                {
                    if (verb == "OpenRead" && IsCommit(path))
                    {
                        Volatile.Write(ref listed, true);
                        if (fault == "io" && path.Name == CommitName(12))
                        {
                            throw new IOException("Commit 12 could not be read");
                        }
                    }
                    return Task.CompletedTask;
                };
                if (fault == "missing")
                {
                    storage.Hidden = path => Volatile.Read(ref listed) && path.Name == CommitName(12);
                }
                var memory = new JobMemory();
                var transfer = new TransferAllowance();
                var (_, failure) = await BuildReadingAhead(storage, 29, readAhead, memory, transfer);
                return (failure, storage, memory, transfer);
            }

            var (sequential, _, _, _) = await Run(1);
            var (failure, storage, memory, transfer) = await Run(8);

            Assert.NotNull(failure);
            Assert.Equal(sequential!.GetType(), failure.GetType());
            Assert.Equal(sequential.Message, failure.Message);
            if (fault == "missing")
            {
                Assert.IsType<DeltaVersionNotAvailableException>(failure);
                Assert.Equal("Commit 12 was removed while it was read", failure.Message);
            }
            else
            {
                Assert.IsType<IOException>(failure);
            }
            Assert.Equal(Enumerable.Range(0, 12).Select(x => (long)x), storage.Read);
            Assert.Subset(storage.Opened.ToHashSet(), Enumerable.Range(13, 7).Select(x => (long)x).ToHashSet());
            AssertReleased(storage, memory, transfer);
        }

        [Fact]
        public async Task EarlierTailDeclineWinsOverALaterFailure()
        {
            async Task<(BuildResult Result, TrackedHookFileStorage Storage, JobMemory Memory, TransferAllowance Transfer)> Run(int readAhead)
            {
                var storage = TrackedHookFileStorage.Over(Memory(nameof(EarlierTailDeclineWinsOverALaterFailure)));
                await DeltaMaintenanceWorkerTests.Commits(storage, 0, 19);
                await storage.WriteText($"/t/_delta_log/{CommitName(5)}", "{\"add\":");
                // Commit 9 goes after the listing
                var listed = false;
                storage.Before = (verb, path) =>
                {
                    if (verb == "OpenRead" && IsCommit(path))
                    {
                        Volatile.Write(ref listed, true);
                    }
                    return Task.CompletedTask;
                };
                storage.Hidden = path => Volatile.Read(ref listed) && path.Name == CommitName(9);
                var memory = new JobMemory();
                var transfer = new TransferAllowance();
                var (result, failure) = await BuildReadingAhead(storage, 19, readAhead, memory, transfer);
                Assert.Null(failure);
                return (result!, storage, memory, transfer);
            }

            var (sequential, _, _, _) = await Run(1);
            var (ahead, storage, memory, transfer) = await Run(8);

            Assert.Equal(BuildStatus.Declined, ahead.Status);
            Assert.Equal(sequential.Status, ahead.Status);
            Assert.Equal(sequential.Reason, ahead.Reason);
            Assert.Equal(DeclineDependency.Tail, ahead.Dependency);
            Assert.Equal(5, ahead.TailCommit!.Version);
            Assert.Equal((sequential.TailCommit!.Version, sequential.TailCommit.Size), (ahead.TailCommit.Version, ahead.TailCommit.Size));
            Assert.Contains(9L, storage.Opened);
            AssertReleased(storage, memory, transfer);
        }

        [Fact]
        public async Task ReplayPastTheDeadlineOpensNothingAfterIt()
        {
            var storage = TrackedHookFileStorage.Over(Memory(nameof(ReplayPastTheDeadlineOpensNothingAfterIt)));
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 9);
            var clock = new FakeTimeProvider(s_now);
            var deadline = clock.GetUtcNow() + TimeSpan.FromHours(6);
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name == CommitName(3))
                {
                    clock.Advance(TimeSpan.FromHours(6));
                }
                return Task.CompletedTask;
            };
            var memory = new JobMemory();
            var transfer = new TransferAllowance();

            var (result, failure) = await BuildReadingAhead(storage, 9, 8, memory, transfer, clock, deadline);

            Assert.Null(failure);
            Assert.Equal(BuildStatus.Declined, result!.Status);
            Assert.Equal(MaintenanceReason.JobDeadline, result.Reason);
            Assert.Equal(new long[] { 0, 1, 2, 3 }, storage.Opened);
            Assert.Equal(new long[] { 0 }, storage.Read);
            AssertReleased(storage, memory, transfer);
        }

        [Fact]
        public async Task CancelledReplayDisposesEveryOpenedCommit()
        {
            var storage = TrackedHookFileStorage.Over(Memory(nameof(CancelledReplayDisposesEveryOpenedCommit)));
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 9);
            using var cancel = new CancellationTokenSource();
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name == CommitName(3))
                {
                    cancel.Cancel();
                }
                return Task.CompletedTask;
            };
            var memory = new JobMemory();
            var transfer = new TransferAllowance();

            var (_, failure) = await BuildReadingAhead(storage, 9, 8, memory, transfer, cancellationToken: cancel.Token);

            Assert.IsAssignableFrom<OperationCanceledException>(failure);
            Assert.Equal(new long[] { 0, 1, 2, 3 }, storage.Opened);
            Assert.Subset(new HashSet<long>() { 0 }, storage.Read.ToHashSet());
            AssertReleased(storage, memory, transfer);
        }
    }
}
