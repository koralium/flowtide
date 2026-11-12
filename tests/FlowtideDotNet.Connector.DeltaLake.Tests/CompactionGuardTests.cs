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
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CompactionGuardTests
    {
        private static readonly DateTimeOffset Start = new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.Zero);
        private const long Minute = 60_000;
        private const long Half = 30 * Minute;

        [Fact]
        public void ASettledCandidateBumpsTheSequenceAndMovesQuietSince()
        {
            var window = new CompactionWindow();
            var guard = NewGuard(window, out _);
            guard.CommitStaged(true, guard.Sequence);

            guard.SettledCandidate(5 * Minute);

            Assert.Equal(1, guard.Sequence);
            Assert.Equal(5 * Minute, guard.QuietSince);
            Assert.Equal(0, guard.Commits);
            Assert.Equal(5 * Minute, window.LastSettledAt);
        }

        [Fact]
        public void QuietNeedsTheWindowAndTheCommits()
        {
            var guard = NewGuard(new CompactionWindow(), out _);
            for (int i = 0; i < 499; i++)
            {
                guard.CommitStaged(true, guard.Sequence);
            }

            Assert.False(guard.IsQuiet(Half));
            guard.CommitStaged(true, guard.Sequence);
            Assert.True(guard.IsQuiet(Half));
            Assert.False(guard.IsQuiet(Half - 1));
        }

        [Theory]
        [InlineData(120L, 240L)]
        [InlineData(31L, 62L)]
        [InlineData(20L * 24 * 60, 30L * 24 * 60)]
        public void ARelapseGrowsTheWindowPastTheQuietSpell(long spellMinutes, long windowMinutes)
        {
            var window = new CompactionWindow();
            var guard = NewGuard(window, out var logger);
            for (int i = 0; i < 500; i++)
            {
                guard.CommitStaged(true, guard.Sequence);
            }

            guard.SettledCandidate(spellMinutes * Minute);

            Assert.Equal(windowMinutes * Minute, window.WindowMs);
            Assert.Single(logger.Entries, x => x.Level == LogLevel.Information && x.Message.Contains("compaction waits"));
            Assert.False(guard.IsQuiet(spellMinutes * Minute));
        }

        [Fact]
        public void ASettledCandidateWhileClosedKeepsTheWindow()
        {
            var window = new CompactionWindow();
            var guard = NewGuard(window, out var logger);

            guard.SettledCandidate(10 * Minute);

            Assert.Equal(Half, window.WindowMs);
            Assert.Empty(logger.Entries);
        }

        [Fact]
        public void AnEpochRestartClosesTheWindow()
        {
            var guard = NewGuard(new CompactionWindow(), out _);
            for (int i = 0; i < 500; i++)
            {
                guard.CommitStaged(true, guard.Sequence);
            }
            Assert.True(guard.IsQuiet(Half));

            guard.Restart(Half);

            Assert.Equal(Half, guard.QuietSince);
            Assert.False(guard.IsQuiet(Half));
            Assert.Equal(0, guard.Commits);
            Assert.Equal(0, guard.Sequence);
            for (int i = 0; i < 500; i++)
            {
                guard.CommitStaged(true, guard.Sequence);
            }
            Assert.False(guard.IsQuiet(2 * Half - 1));
            Assert.True(guard.IsQuiet(2 * Half));
            guard.Restart(Minute);
            Assert.Equal(Half, guard.QuietSince);
        }

        [Theory]
        [InlineData(1_000L, Half)]
        [InlineData(5 * 60 * Minute, 5 * 60 * Minute)]
        public void AStoredWindowIsAtLeastHalfTheColdAfter(long stored, long expected)
        {
            var window = new CompactionWindow() { WindowMs = stored };

            NewGuard(window, out _);

            Assert.Equal(expected, window.WindowMs);
        }

        [Fact]
        public void UniformDeletesNeverGoQuiet()
        {
            var guard = NewGuard(new CompactionWindow(), out _);
            for (long second = 1; second <= 3_000; second++)
            {
                var now = second * 1000;
                DeleteCommit(guard, now, now - 1_000, now - 2 * Half);
                Assert.False(guard.IsQuiet(now));
            }

            Assert.Equal(3_000, guard.Sequence);
            Assert.Equal(0, guard.Commits);
        }

        [Fact]
        public void RecentKeyDeletesGoQuietAfterTheWindow()
        {
            var guard = NewGuard(new CompactionWindow(), out _);
            var quietAt = 0L;
            for (long second = 1; second <= 3_000 && quietAt == 0; second++)
            {
                var now = second * 1000;
                DeleteCommit(guard, now, now - 1_000, now - Half + 1_000);
                if (guard.IsQuiet(now))
                {
                    quietAt = second;
                }
            }

            Assert.Equal(Half / 1000, quietAt);
            Assert.Equal(0, guard.Sequence);
        }

        [Fact]
        public void ABurstLearnsItsPeriodAndTheNextBurstFindsTheTableClosed()
        {
            var window = new CompactionWindow();
            var guard = NewGuard(window, out _);
            var burstAt = new long[] { 2_000, 3_500, 5_000 };
            var quietAtBurst = new List<bool>();
            for (long second = 1; second <= 5_000; second++)
            {
                var now = second * 1000;
                if (burstAt.Contains(second))
                {
                    quietAtBurst.Add(guard.IsQuiet(now));
                    DeleteCommit(guard, now, now - 2 * Half);
                }
                else
                {
                    guard.CommitStaged(true, guard.Sequence);
                }
            }

            Assert.Equal(new[] { true, false, false }, quietAtBurst);
            Assert.True(window.WindowMs >= 2 * 2_000_000, $"window {window.WindowMs}");
        }

        [Fact]
        public void QuietSinceKeepsTheLastSettledTimeAcrossAClockStep()
        {
            var window = new CompactionWindow() { LastSettledAt = 40 * Minute };
            var restored = new CompactionGuard(window, TimeSpan.FromHours(1), new CompactionLimits(), 25 * Minute, new DeltaMaintenanceWorkerTests.CollectingLogger(), "t");
            for (int i = 0; i < 500; i++)
            {
                restored.CommitStaged(true, restored.Sequence);
            }

            Assert.Equal(40 * Minute, restored.QuietSince);
            Assert.False(restored.IsQuiet(69 * Minute));
            Assert.True(restored.IsQuiet(70 * Minute));
            restored.SettledCandidate(20 * Minute);
            Assert.Equal(40 * Minute, restored.QuietSince);
            Assert.Equal(40 * Minute, window.LastSettledAt);
            var reopenedLater = new CompactionGuard(window, TimeSpan.FromHours(1), new CompactionLimits(), 50 * Minute, new DeltaMaintenanceWorkerTests.CollectingLogger(), "t");
            Assert.Equal(50 * Minute, reopenedLater.QuietSince);
        }

        [Fact]
        public void OnlyCleanDataCommitsCount()
        {
            var guard = NewGuard(new CompactionWindow(), out _);
            var sequence = guard.Sequence;

            guard.CommitStaged(false, sequence);
            Assert.Equal(0, guard.Commits);
            guard.SettledCandidate(Minute);
            guard.CommitStaged(true, sequence);
            Assert.Equal(0, guard.Commits);
            guard.CommitStaged(true, guard.Sequence);
            Assert.Equal(1, guard.Commits);
        }

        [Fact]
        public void AGroupIsCheckedWholeBeforeAnyCandidateIsRead()
        {
            var guard = NewGuard(new CompactionWindow(), out _);
            var now = 100 * Minute;
            guard.AddOutput("output.parquet");
            var records = new Dictionary<int, DeltaFileRecord>()
            {
                [0] = new DeltaFileRecord() { Path = "young.parquet", ModificationTime = now },
                [1] = new DeltaFileRecord() { Path = "old.parquet", ModificationTime = now - Half },
                [2] = new DeltaFileRecord() { Path = "output.parquet", ModificationTime = now },
                [3] = new DeltaFileRecord() { Path = "later.parquet", ModificationTime = 0 }
            };
            var candidates = new List<int>() { 0, 1, 2, 3 };

            var settled = guard.CheckGroup(candidates, 0, 3, records, now);

            Assert.Equal(2, settled);
            Assert.Equal(1, guard.Sequence);
            Assert.Equal(0, guard.CheckGroup(candidates, 0, 1, records, now));
            Assert.Equal(1, guard.Sequence);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ADeleteOfASettledFileClosesTheTable(bool enabled)
        {
            var testName = $"{nameof(ADeleteOfASettledFileClosesTheTable)}_{enabled}";
            using var storage = Files.Of.InternalMemory($"./{testName}");
            var clock = new FakeTimeProvider(Start);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.TimeProvider = clock;
                options.EnableAutoCompaction = enabled;
                options.CompactionColdAfter = TimeSpan.FromHours(1);
                options.CompactionLimitsOverride = new CompactionLimits() { QuietCommits = 1 };
            });
            stream.AddUsers(Users(1, 10));
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            clock.Advance(TimeSpan.FromMinutes(1));
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);
            var sink = stream.Sinks[^1];
            var youngGuard = sink.CompactionGuardForTests;
            var youngSettled = sink.DeleteStatisticsForTests!.SettledCandidates;
            var youngSequence = youngGuard?.Sequence;
            var youngCommits = youngGuard?.Commits;

            clock.Advance(TimeSpan.FromHours(1));
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(enabled, sink.HasCompactionStateForTests);
            Assert.Equal(enabled, sink.CompactionGuardForTests != null);
            Assert.Equal(0, youngSettled);
            if (enabled)
            {
                Assert.Equal(0, youngSequence);
                Assert.Equal(1, youngCommits);
                Assert.Equal(1, sink.CompactionGuardForTests!.Sequence);
                Assert.Equal(0, sink.CompactionGuardForTests.Commits);
                Assert.True(sink.DeleteStatisticsForTests!.SettledCandidates > 0);
                clock.Advance(TimeSpan.FromHours(1));
                Assert.False(sink.CompactionGuardForTests.IsQuiet(clock.GetUtcNow().ToUnixTimeMilliseconds()));
                stream.AddUsers(Users(11, 2));
                await WaitForVersion(storage, "test", stream, 3);
                Assert.Equal(1, sink.CompactionGuardForTests.Commits);
            }
            else
            {
                Assert.Equal(0, sink.DeleteStatisticsForTests!.SettledCandidates);
            }
        }

        [Fact]
        public async Task AGroupTheDeleteNeverReadCountsAsSettled()
        {
            var testName = nameof(AGroupTheDeleteNeverReadCountsAsSettled);
            using var storage = Files.Of.InternalMemory($"./{testName}");
            var clock = new FakeTimeProvider(Start);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.TimeProvider = clock;
                options.EnableAutoCompaction = true;
                options.CompactionColdAfter = TimeSpan.FromDays(1);
                options.DeleteLimitsOverride = new DeleteLimits() { RecordGroup = 1 };
            });
            var first = Users(1, 1, "a").Concat(Users(100, 1, "a")).ToList();
            stream.AddUsers(first);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.AddUsers(Users(3, 1, "a").Concat(Users(150, 1, "a")).ToList());
            await WaitForVersion(storage, "test", stream, 1);

            stream.DeleteUser(first[1]);
            await WaitForVersion(storage, "test", stream, 2);

            var sink = stream.Sinks[^1];
            Assert.Equal(2, sink.DeleteStatisticsForTests!.Candidates);
            Assert.Equal(1, sink.DeleteStatisticsForTests.SettledCandidates);
            Assert.Equal(1, sink.CompactionGuardForTests!.Sequence);
        }

        [Fact]
        public async Task APartlyReadGroupChecksItsUnreadCandidates()
        {
            var testName = nameof(APartlyReadGroupChecksItsUnreadCandidates);
            using var storage = Files.Of.InternalMemory($"./{testName}");
            var clock = new FakeTimeProvider(Start);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.TimeProvider = clock;
                options.EnableAutoCompaction = true;
                options.CompactionColdAfter = TimeSpan.FromHours(1);
                options.DeleteLimitsOverride = new DeleteLimits() { RecordGroup = 2 };
            });
            var removable = Users(2, 1, "a")[0];
            stream.AddUsers(new[] { removable });
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.AddUsers(Users(1, 1, "a").Concat(Users(10, 1, "a")).ToList());
            await WaitForVersion(storage, "test", stream, 1);

            clock.Advance(TimeSpan.FromMinutes(1));
            stream.DeleteUser(removable);
            await WaitForVersion(storage, "test", stream, 2);
            var sink = stream.Sinks[^1];
            var young = (sink.DeleteStatisticsForTests!.Candidates, sink.DeleteStatisticsForTests.FilesScanned, sink.DeleteStatisticsForTests.SettledCandidates, sink.CompactionGuardForTests!.Sequence);

            clock.Advance(TimeSpan.FromHours(1));
            var recycled = Users(5, 1, "a")[0];
            stream.AddUsers(new[] { recycled });
            await WaitForVersion(storage, "test", stream, 3);
            clock.Advance(TimeSpan.FromMinutes(1));
            stream.DeleteUser(recycled);
            await WaitForVersion(storage, "test", stream, 4);

            Assert.Equal((2L, 1L, 0L, 0L), young);
            Assert.Equal(2, sink.DeleteStatisticsForTests!.Candidates);
            Assert.Equal(1, sink.DeleteStatisticsForTests.FilesScanned);
            Assert.Equal(1, sink.DeleteStatisticsForTests.SettledCandidates);
            Assert.Equal(1, sink.CompactionGuardForTests!.Sequence);
        }

        [Fact]
        public async Task TheWindowSurvivesARestoreAndTheCountersRestartClosed()
        {
            var testName = nameof(TheWindowSurvivesARestoreAndTheCountersRestartClosed);
            using var storage = Files.Of.InternalMemory($"./{testName}");
            var clock = new FakeTimeProvider(Start);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.TimeProvider = clock;
                options.EnableAutoCompaction = true;
                options.CompactionColdAfter = TimeSpan.FromHours(1);
                options.CompactionLimitsOverride = new CompactionLimits() { QuietCommits = 2 };
            });
            stream.AddUsers(Users(1, 10));
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            clock.Advance(TimeSpan.FromMinutes(1));
            stream.AddUsers(Users(11, 2));
            await WaitForVersion(storage, "test", stream, 1);
            clock.Advance(TimeSpan.FromMinutes(1));
            stream.AddUsers(Users(13, 2));
            await WaitForVersion(storage, "test", stream, 2);
            clock.Advance(TimeSpan.FromMinutes(40));
            var sink = stream.Sinks[^1];
            var burst = clock.GetUtcNow().ToUnixTimeMilliseconds();
            var quietBeforeBurst = sink.CompactionGuardForTests!.IsQuiet(burst);
            var spell = burst - sink.CompactionGuardForTests.QuietSince;
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 3);
            var grown = sink.CompactionWindowForTests!.WindowMs;
            var settledAt = sink.CompactionWindowForTests.LastSettledAt;
            var epoch = sink.CatalogEpochForTests;

            await stream.Crash();
            clock.Advance(TimeSpan.FromMinutes(1));
            stream.AddUsers(Users(15, 2));
            await WaitForVersion(storage, "test", stream, 4);

            var restored = stream.Sinks[^1];
            Assert.True(quietBeforeBurst);
            Assert.True(epoch > 0);
            Assert.True(restored.CatalogEpochForTests > epoch);
            Assert.True(spell > Half, $"spell {spell}");
            Assert.Equal(2 * spell, grown);
            Assert.Equal(grown, restored.CompactionWindowForTests!.WindowMs);
            Assert.Equal(settledAt, restored.CompactionWindowForTests.LastSettledAt);
            Assert.Equal(1, restored.CompactionGuardForTests!.Commits);
            Assert.Equal(0, restored.CompactionGuardForTests.Sequence);
            Assert.False(restored.CompactionGuardForTests.IsQuiet(clock.GetUtcNow().ToUnixTimeMilliseconds()));
        }

        // One delete commit in WriteCommit's order, over files written at these times
        private static void DeleteCommit(CompactionGuard guard, long now, params long[] written)
        {
            var sequence = guard.Sequence;
            var records = new Dictionary<int, DeltaFileRecord>();
            for (int i = 0; i < written.Length; i++)
            {
                records[i] = new DeltaFileRecord() { Path = $"part-{i}.parquet", Size = 1, ModificationTime = written[i], PartitionValues = new Dictionary<string, string>() };
            }
            guard.CheckGroup(records.Keys.ToList(), 0, records.Count, records, now);
            guard.CommitStaged(true, sequence);
        }

        private static CompactionGuard NewGuard(CompactionWindow window, out DeltaMaintenanceWorkerTests.CollectingLogger logger)
        {
            logger = new DeltaMaintenanceWorkerTests.CollectingLogger();
            return new CompactionGuard(window, TimeSpan.FromHours(1), new CompactionLimits(), 0, logger, "t");
        }

        private static List<User> Users(int first, int count, string? name = null)
        {
            return Enumerable.Range(first, count).Select(x => new User() { UserKey = x, FirstName = name ?? $"First{x}" }).ToList();
        }
    }
}
