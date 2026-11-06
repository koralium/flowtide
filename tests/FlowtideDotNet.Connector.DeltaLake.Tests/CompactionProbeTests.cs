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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using Stowage;
using Stowage.Impl;
using System.Text.Json;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection("Probes")]
    public class CompactionProbeTests
    {
        private static readonly CompactionFixture s_tiny = new CompactionFixture("Tiny", "tiny", 4_000, 200, 20, 1, DeletePlace.Uniform, 5, true, false);
        private readonly ITestOutputHelper _output;

        public CompactionProbeTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void WorkloadReplaysTheSameRowsAndDeletes()
        {
            var first = Rows(new CompactionWorkload(s_tiny));
            var second = Rows(new CompactionWorkload(s_tiny));

            Assert.Equal(first, second);
            Assert.Equal(4_000 + 25 * 200, first.Count(x => !x.Deleted));
        }

        [Fact]
        public void SkippedCommitsLeaveTheSameNextCommit()
        {
            var played = new CompactionWorkload(s_tiny);
            played.Base().ToList();
            for (int i = 0; i < 7; i++)
            {
                played.Next();
            }
            var skipped = new CompactionWorkload(s_tiny);
            skipped.Skip(7);

            var expected = played.Next();
            var actual = skipped.Next();

            Assert.Equal(expected.Inserts, actual.Inserts);
            Assert.Equal(expected.Deletes, actual.Deletes);
        }

        [Fact]
        public void RecentDeletesStayInTheNewestTenth()
        {
            var workload = new CompactionWorkload(s_tiny with { Place = DeletePlace.Recent });
            workload.Skip(0);
            Assert.Equal(4_000, workload.LiveRows);

            var (_, deletes) = workload.Next();

            Assert.Equal(20, deletes.Count);
            Assert.All(deletes, x => Assert.InRange(x.UserKey, 3_600, 4_000));
        }

        [Fact]
        public void PreAgeSpreadsDebtOverTheBaseFiles()
        {
            var workload = new CompactionWorkload(s_tiny with { BaseRows = 40_000, PreAge = 10 });
            workload.Skip(0);
            var deleted = new List<int>();
            for (int i = 0; i < 10; i++)
            {
                deleted.AddRange(workload.Next().Deletes.Select(x => x.UserKey));
            }

            var perFile = deleted.GroupBy(x => (x - 1) / 200).Select(x => x.Count()).ToList();
            Assert.All(perFile, x => Assert.InRange(x, 1, 19));
            Assert.InRange(perFile.Max() - perFile.Min(), 10, 19);
            Assert.Equal(20, workload.Next().Deletes.Count);
        }

        [Fact]
        public void ProbeClockAdvancesOneSecondPerCommit()
        {
            var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new ProbeClock(start);

            clock.Advance();
            clock.Advance();

            Assert.Equal(start.AddSeconds(2), clock.GetUtcNow());
        }

        [Fact]
        public async Task ChargedReadsAndWritesWaitForTheirBytes()
        {
            var waits = new List<TimeSpan>();
            using var storage = new HookFileStorage(Files.Of.InternalMemory($"./charged-{Guid.NewGuid():N}"))
            {
                BytesPerSecond = 100_000_000,
                Wait = (time, _) =>
                {
                    waits.Add(time);
                    return Task.CompletedTask;
                }
            };
            var bytes = new byte[100_000];

            await using (var write = await storage.OpenWrite("/data.bin"))
            {
                await write.WriteAsync(bytes);
            }
            using var read = await storage.OpenRead("/data.bin");

            Assert.Equal(new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1) }, waits);
        }

        [Fact]
        public void SlopeIsNullWhenRowsNeverVary()
        {
            Assert.Null(CompactionProbe.Slope(new List<(long, double)> { (0, 5), (0, 7) }));
            Assert.Equal(2.0, CompactionProbe.Slope(new List<(long, double)> { (1, 3), (2, 5), (3, 7) }));
            Assert.Equal(2.0, CompactionProbe.Slope(new List<(long, double)> { (0, 5), (2, 9) }));
        }

        [Fact]
        public void PercentilesAreLinearBetweenRanks()
        {
            var values = new double[] { 4, 1, 3, 2 };

            Assert.Equal(2.5, ProbeKit.Percentile(values, 0.5));
            Assert.Equal(3.97, ProbeKit.Percentile(values, 0.99), 6);
            var (low, high) = ProbeKit.Bootstrap(new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 100 }, 0.5, 7);
            Assert.InRange(low, 3, 5.5);
            Assert.InRange(high, 5.5, 8);
        }

        [Fact]
        public void TransferChargesBytesAtTheRate()
        {
            Assert.Equal(TimeSpan.FromSeconds(1), HookFileStorage.Transfer(100_000_000, 100_000_000));
            Assert.Equal(TimeSpan.FromMilliseconds(2), HookFileStorage.Transfer(200_000, 100_000_000));
        }

        [Fact]
        public void SourceQueryReadsEveryColumn()
        {
            var query = CompactionProbe.SourceQuery();

            Assert.All(CompactionFixture.ColumnNames, x => Assert.Contains($"{x} IS NOT NULL", query));
        }

        [Fact]
        public async Task ObjectStoreTablesHideTheLocalDiskPath()
        {
            await using var local = new ProbeTable("local-" + Guid.NewGuid().ToString("N"), ProbeTable.NewRoot(), null, keepFiles: false);
            await using var remote = new ProbeTable("remote-" + Guid.NewGuid().ToString("N"), ProbeTable.NewRoot(), null, keepFiles: false, objectStore: true);

            Assert.IsAssignableFrom<ILocalDiskFileStorage>(local.Storage);
            Assert.IsNotAssignableFrom<ILocalDiskFileStorage>(remote.Storage);
        }

        [Fact]
        public async Task TinyFixtureRunsEndToEnd()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(s_tiny, root, output));
                Assert.Equal(ManualRun.Passed, await CompactionProbe.RunAsync(s_tiny, root, root, new CompactionProbe.RunSettings(30, 5, 0, false, true), output));

                var lines = output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
                _output.WriteLine(lines[^1]);
                var summary = JsonDocument.Parse(lines[^1]).RootElement;
                Assert.Equal("summary", summary.GetProperty("record").GetString());
                Assert.Equal(25, summary.GetProperty("commits").GetInt32());
                Assert.Equal(25, summary.GetProperty("overruns").GetInt32());
                Assert.Equal(summary.GetProperty("expectedLiveRows").GetInt64(), summary.GetProperty("liveRows").GetInt64());
                Assert.Equal(summary.GetProperty("expectedLiveRows").GetInt64(), summary.GetProperty("sourceRows").GetInt64());
                Assert.Equal(0, summary.GetProperty("missingFiles").GetInt32());
                Assert.True(summary.GetProperty("checkpoint").GetProperty("p50").GetDouble() > 0);
                Assert.True(summary.GetProperty("source").GetProperty("p50").GetDouble() > 0);
                Assert.True(summary.GetProperty("rowsScanned").GetDouble() > 0);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public void RecentDeletesNeverTakeRowsOfTheSameCommit()
        {
            var workload = new CompactionWorkload(s_tiny with { BaseRows = 400, Place = DeletePlace.Recent });
            workload.Skip(0);

            var (inserts, deletes) = workload.Next();

            Assert.Equal(20, deletes.Count);
            Assert.DoesNotContain(deletes, x => inserts.Any(y => y.UserKey == x.UserKey));
        }

        [Fact]
        public void KrandKeepsKeyColumnsOutOfThePruningColumns()
        {
            var sql = CompactionFixture.All["Krand"].Sql();
            var columns = sql[(sql.IndexOf('(') + 1)..sql.IndexOf(')')]
                .Split(',', StringSplitOptions.TrimEntries)
                .Select(x => x.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0])
                .Take(new DeltaLakeOptions() { StorageLocation = null! }.StatsPruningColumns)
                .ToList();

            Assert.DoesNotContain("userkey", columns);
            Assert.DoesNotContain("managerkey", columns);
        }

        [Theory]
        [InlineData("store", "not-a-store")]
        [InlineData("source", "not-a-source-mode")]
        [InlineData("pace", "NaN")]
        [InlineData("pace", "Infinity")]
        public async Task InvalidRunOptionsAreRejectedBeforeOpeningAFixture(string key, string value)
        {
            var options = new Dictionary<string, string>
            {
                ["fixture"] = "A",
                ["root"] = Path.Combine(Path.GetTempPath(), "absent-fixture-" + Guid.NewGuid().ToString("N")),
                ["commits"] = "1",
                ["discard"] = "0",
                [key] = value
            };
            using var output = new StringWriter();

            Assert.Equal(ManualRun.HarnessError, await CompactionProbe.RunAsync(options, output));
            Assert.StartsWith(key == "pace" ? "Need " : $"Unknown {key}", output.ToString());
        }

        [Fact]
        public async Task InvalidBuildOptionsAreRejected()
        {
            using var output = new StringWriter();

            Assert.Equal(ManualRun.HarnessError, await CompactionProbe.BuildAsync(new Dictionary<string, string>(), output));
            Assert.Equal(ManualRun.HarnessError, await CompactionProbe.BuildAsync(new Dictionary<string, string> { ["fixture"] = "Nope", ["root"] = "." }, output));
        }

        [Fact]
        public void BaseShapeChangesWithItsSize()
        {
            var fixture = CompactionFixture.All["K"];

            Assert.NotEqual(CompactionProbe.Shape(fixture, false), CompactionProbe.Shape(fixture with { BaseRows = 4_000_000 }, false));
            Assert.NotEqual(CompactionProbe.Shape(fixture, false), CompactionProbe.Shape(fixture with { Inserts = 800 }, false));
            Assert.NotEqual(CompactionProbe.Shape(fixture, false), CompactionProbe.Shape(fixture with { DeletionVectors = false }, false));
        }

        [Fact]
        public async Task FixturesBuiltByOtherCodeAreNotRun()
        {
            var fixture = new CompactionFixture("Append", "append", 200, 100, 0, 1, DeletePlace.None, 0, true, false);
            var root = ProbeTable.NewRoot();
            var source = Path.Combine(root, "base-append");
            Directory.CreateDirectory(source);
            File.WriteAllText(Path.Combine(source, "manifest.json"), JsonSerializer.Serialize(new CompactionProbe.FixtureManifest("append", 0, 0, "older rows")));
            try
            {
                using var output = new StringWriter();

                Assert.Equal(ManualRun.HarnessError, await CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(3, 0, 0, false, false), output));
                Assert.False(Directory.Exists(Path.Combine(root, "compaction-runs")));
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public void LinkedFilesShareTheirBytes()
        {
            var root = ProbeTable.NewRoot();
            try
            {
                var from = Path.Combine(root, "part-a.parquet");
                var to = Path.Combine(root, "part-b.parquet");
                File.WriteAllBytes(from, new byte[] { 1, 2, 3 });

                CompactionProbe.LinkOrCopy(from, to);
                using (var write = new FileStream(to, FileMode.Open, FileAccess.Write))
                {
                    write.WriteByte(9);
                }

                Assert.Equal(new byte[] { 9, 2, 3 }, File.ReadAllBytes(from));
                File.Delete(from);
                Assert.Equal(new byte[] { 9, 2, 3 }, File.ReadAllBytes(to));
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public void ProjectedBytesCountTheLogAndTheRunsGrowth()
        {
            var root = ProbeTable.NewRoot();
            var log = Path.Combine(root, CompactionFixture.Table, "_delta_log");
            Directory.CreateDirectory(log);
            var first = "{\"add\":{\"path\":\"part-0.parquet\",\"size\":4000}}\n";
            var second = "{\"commitInfo\":{}}\n{\"add\":{\"path\":\"part-1.parquet\",\"size\":6000}}\n{\"remove\":{\"path\":\"part-0.parquet\"}}\n";
            File.WriteAllText(Path.Combine(log, $"{0:D20}.json"), first);
            File.WriteAllText(Path.Combine(log, $"{1:D20}.json"), second);
            File.WriteAllBytes(Path.Combine(log, $"{1:D20}.checkpoint.parquet"), new byte[1_000]);
            File.WriteAllBytes(Path.Combine(root, CompactionFixture.Table, "part-1.parquet"), new byte[500]);
            try
            {
                var linked = CompactionProbe.ProjectedBytes(root, s_tiny with { Inserts = 10 }, 50, linked: true);
                var copied = CompactionProbe.ProjectedBytes(root, s_tiny with { Inserts = 10 }, 50, linked: false);

                var expected = first.Length + second.Length + 1_000 + 2 * 1_000 + 50 * 1_000 + 120 * (second.Length + 5_000);
                Assert.Equal(expected, linked);
                Assert.Equal(expected + 500, copied);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public void ProjectedBytesSkipVectorUpdates()
        {
            var root = ProbeTable.NewRoot();
            var log = Path.Combine(root, CompactionFixture.Table, "_delta_log");
            Directory.CreateDirectory(log);
            var first = "{\"add\":{\"path\":\"part-0.parquet\",\"size\":4000}}\n";
            var second = "{\"remove\":{\"path\":\"part-0.parquet\"}}\n{\"add\":{\"path\":\"part-0.parquet\",\"size\":4000,\"deletionVector\":{\"storageType\":\"u\",\"pathOrInlineDv\":\"x\",\"sizeInBytes\":40,\"cardinality\":1}}}\n{\"add\":{\"path\":\"part-1.parquet\",\"size\":6000}}\n";
            File.WriteAllText(Path.Combine(log, $"{0:D20}.json"), first);
            File.WriteAllText(Path.Combine(log, $"{1:D20}.json"), second);
            try
            {
                var bytes = CompactionProbe.ProjectedBytes(root, s_tiny with { Inserts = 10 }, 50, linked: true);

                Assert.Equal(first.Length + second.Length + 50 * 1_000 + 120 * (second.Length + 5_020), bytes);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public void PruningNeverDropsAVersionNotReadYet()
        {
            var root = ProbeTable.NewRoot();
            var log = Path.Combine(root, CompactionFixture.Table, "_delta_log");
            Directory.CreateDirectory(log);
            foreach (var version in Enumerable.Range(0, 13))
            {
                File.WriteAllText(Path.Combine(log, $"{version:D20}.json"), "");
            }
            File.WriteAllText(Path.Combine(log, "_last_checkpoint"), "{\"version\":12,\"size\":3}");
            try
            {
                CompactionProbe.PruneLog(root, 9);

                Assert.True(File.Exists(Path.Combine(log, $"{9:D20}.json")));
                Assert.False(File.Exists(Path.Combine(log, $"{8:D20}.json")));
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public void PruningKeepsTheLogFromTheLastCheckpoint()
        {
            var root = ProbeTable.NewRoot();
            var log = Path.Combine(root, CompactionFixture.Table, "_delta_log");
            Directory.CreateDirectory(log);
            foreach (var version in Enumerable.Range(0, 13))
            {
                File.WriteAllText(Path.Combine(log, $"{version:D20}.json"), "");
            }
            File.WriteAllText(Path.Combine(log, $"{5:D20}.checkpoint.parquet"), "");
            File.WriteAllText(Path.Combine(log, $"{10:D20}.checkpoint.parquet"), "");
            File.WriteAllText(Path.Combine(log, "_last_checkpoint"), "{\"version\":10,\"size\":3}");
            try
            {
                CompactionProbe.PruneLog(root);

                var left = Directory.EnumerateFiles(log).Select(Path.GetFileName).Order(StringComparer.Ordinal);
                Assert.Equal(new[] { $"{10:D20}.checkpoint.parquet", $"{10:D20}.json", $"{11:D20}.json", $"{12:D20}.json", "_last_checkpoint" }, left);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task FailedAndKilledRunsLeaveNoRunDirectory()
        {
            if (OperatingSystem.IsWindows())
            {
                return;
            }
            var fixture = new CompactionFixture("Append", "append", 200, 100, 0, 1, DeletePlace.None, 0, true, false);
            var root = ProbeTable.NewRoot();
            var source = Path.Combine(root, "base-append");
            Directory.CreateDirectory(source);
            Directory.CreateDirectory(Path.Combine(source, CompactionFixture.Table, "_delta_log"));
            File.CreateSymbolicLink(Path.Combine(source, "missing"), Path.Combine(root, "absent"));
            File.WriteAllText(Path.Combine(source, "manifest.json"), JsonSerializer.Serialize(new CompactionProbe.FixtureManifest("append", 0, 0, CompactionProbe.Shape(fixture, false))));
            var killed = Path.Combine(root, "compaction-runs", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path.Combine(killed, "run"));
            File.WriteAllText(killed + ".lock", "");
            try
            {
                using var output = new StringWriter();

                var error = await Record.ExceptionAsync(() => CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(3, 0, 0, false, false), output));

                Assert.NotNull(error);
                Assert.Empty(Directory.EnumerateDirectories(Path.Combine(root, "compaction-runs")));
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task RefusedRunsStillSweepKilledCopies()
        {
            if (OperatingSystem.IsWindows())
            {
                return;
            }
            var fixture = new CompactionFixture("Append", "append", 200, 100, 0, 1, DeletePlace.None, 0, true, false);
            var root = ProbeTable.NewRoot();
            var source = Path.Combine(root, "base-append");
            var log = Path.Combine(source, CompactionFixture.Table, "_delta_log");
            Directory.CreateDirectory(log);
            using (var huge = File.Create(Path.Combine(log, $"{0:D20}.json")))
            {
                huge.SetLength(1L << 40);
            }
            File.WriteAllText(Path.Combine(source, "manifest.json"), JsonSerializer.Serialize(new CompactionProbe.FixtureManifest("append", 0, 0, CompactionProbe.Shape(fixture, false))));
            var killed = Path.Combine(root, "compaction-runs", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path.Combine(killed, "run"));
            File.WriteAllText(killed + ".lock", "");
            try
            {
                using var output = new StringWriter();

                Assert.Equal(ManualRun.HarnessError, await CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(3, 0, 0, false, false), output));
                Assert.StartsWith("Scratch has", output.ToString());
                Assert.False(Directory.Exists(killed));
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task AppendOnlyRunWritesItsSummary()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var fixture = new CompactionFixture("Append", "append", 400, 200, 0, 1, DeletePlace.None, 0, true, false);
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(fixture, root, output));

                Assert.Equal(ManualRun.Passed, await CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(3, 0, 0, false, false), output));

                var summary = JsonDocument.Parse(output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries)[^1]).RootElement;
                Assert.Equal("summary", summary.GetProperty("record").GetString());
                Assert.Equal(JsonValueKind.Null, summary.GetProperty("cRowMicroseconds").ValueKind);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task InterruptedPreAgeIsNotTakenAsBuilt()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(s_tiny with { PreAge = 0 }, root, output));
                var baseDir = Path.Combine(root, "base-" + s_tiny.Base);
                var dir = Path.Combine(root, s_tiny.Name);
                foreach (var file in Directory.EnumerateFiles(baseDir, "*", SearchOption.AllDirectories))
                {
                    var target = Path.Combine(dir, Path.GetRelativePath(baseDir, file));
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    File.Copy(file, target);
                }

                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(s_tiny, root, output));

                var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "manifest.json"))).RootElement;
                Assert.Equal(s_tiny.Name, manifest.GetProperty("Fixture").GetString());
                Assert.Equal(s_tiny.PreAge, manifest.GetProperty("Commits").GetInt32());
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task FilesPerHourCountsNetLiveFiles()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var fixture = s_tiny with { Name = "TinyDvoff", Base = "tiny-dvoff", BaseRows = 400, Deletes = 5, PreAge = 0, DeletionVectors = false };
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(fixture, root, output));

                Assert.Equal(ManualRun.Passed, await CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(4, 1, 0, false, false), output));

                var summary = JsonDocument.Parse(output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries)[^1]).RootElement;
                Assert.Equal(3_600.0, summary.GetProperty("filesPerHour").GetDouble(), 3);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task RepresentativeFilesAreAboutOneHundredBytesPerRow()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var fixture = new CompactionFixture("Width", "width", 4_000, 2_000, 0, 1, DeletePlace.None, 0, true, false);
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(fixture, root, output));

                using var storage = Files.Of.LocalDisk(Path.Combine(root, "base-width"));
                var table = await DeltaTransactionReader.ReadTable(storage, CompactionFixture.Table);
                var width = table!.AddFiles.Sum(x => x.Size) / (double)fixture.BaseRows;
                _output.WriteLine($"{width:F1} B/row");
                Assert.InRange(width, 80, 125);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task TableTimeAndChargedStoreReachTheSink()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var fixture = new CompactionFixture("Timed", "timed", 400, 200, 0, 1, DeletePlace.None, 3, true, false);
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(fixture, root, output));
                using var storage = Files.Of.LocalDisk(Path.Combine(root, "Timed"));
                var table = await DeltaTransactionReader.ReadTable(storage, CompactionFixture.Table);
                var times = table!.AddFiles.Select(x => x.ModificationTime).Order().ToList();
                var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(root, "Timed", "manifest.json"))).RootElement;

                Assert.Equal(5, times.Count);
                Assert.All(times.Zip(times.Skip(1)), x => Assert.Equal(1_000, x.Second - x.First));
                Assert.Equal(new DateTimeOffset(manifest.GetProperty("ClockTicks").GetInt64(), TimeSpan.Zero).ToUnixTimeMilliseconds(), times[^1]);

                Assert.Equal(ManualRun.Passed, await CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(3, 0, 0, true, false), output));

                var summary = JsonDocument.Parse(output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries)[^1]).RootElement;
                Assert.Equal("azure-like", summary.GetProperty("store").GetString());
                Assert.True(summary.GetProperty("commitVersion").GetProperty("p50").GetDouble() >= 20);
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task PublishedFixturesHoldOnlyLiveFiles()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var fixture = s_tiny with { Name = "Vacuumed", Base = "vacuumed", PreAge = 60 };
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(fixture, root, output));

                var dir = Path.Combine(root, "Vacuumed", CompactionFixture.Table);
                using var storage = Files.Of.LocalDisk(Path.Combine(root, "Vacuumed"));
                var table = await DeltaTransactionReader.ReadTable(storage, CompactionFixture.Table);
                var used = table!.AddFiles.Select(x => x.Path).Concat(table.AddFiles.Select(x => x.DeletionVector?.AbsolutePath).OfType<string>()).ToHashSet();
                var onDisk = Directory.EnumerateFiles(dir).Select(Path.GetFileName).Where(x => x!.StartsWith("part-") || x.StartsWith("deletion_vector_")).ToHashSet();
                Assert.Equal(used.Order(), onDisk.Order());
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        [Fact]
        public async Task PruningKeepsEveryLiveRow()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var fixture = s_tiny with { Name = "Pruned", Base = "pruned", Place = DeletePlace.Recent };
            var root = ProbeTable.NewRoot();
            try
            {
                using var output = new StringWriter();
                Assert.Equal(ManualRun.Passed, await CompactionProbe.BuildAsync(fixture, root, output));
                Assert.Equal(ManualRun.Passed, await CompactionProbe.RunAsync(fixture, root, root, new CompactionProbe.RunSettings(150, 0, 0, false, true), output));

                var summary = JsonDocument.Parse(output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries)[^1]).RootElement;
                _output.WriteLine(summary.ToString());
                Assert.True(summary.GetProperty("prunedFiles").GetInt32() > 0);
                Assert.Equal(0, summary.GetProperty("missingFiles").GetInt32());
                Assert.Equal(summary.GetProperty("expectedLiveRows").GetInt64(), summary.GetProperty("liveRows").GetInt64());
                Assert.Equal(summary.GetProperty("expectedLiveRows").GetInt64(), summary.GetProperty("sourceRows").GetInt64());
                if (summary.GetProperty("duckDbRows").ValueKind == JsonValueKind.Number)
                {
                    Assert.Equal(summary.GetProperty("expectedLiveRows").GetInt64(), summary.GetProperty("duckDbRows").GetInt64());
                }
            }
            finally
            {
                Directory.Delete(root, true);
            }
        }

        private static List<(User Row, bool Deleted)> Rows(CompactionWorkload workload)
        {
            var rows = new List<(User, bool)>();
            foreach (var batch in workload.Base())
            {
                rows.AddRange(batch.Select(x => (x, false)));
            }
            for (int i = 0; i < 25; i++)
            {
                var (inserts, deletes) = workload.Next();
                rows.AddRange(inserts.Select(x => (x, false)));
                rows.AddRange(deletes.Select(x => (x, true)));
            }
            return rows;
        }
    }
}
