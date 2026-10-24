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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AzureOrphanDirectoryTests
    {
        // The DELETE request lines, in order
        private static List<string> Deletes(LoopbackObjectStore server) => server.Requests.Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal)).ToList();

        private static void AssertCompleted(List<CleanupResult> turns)
        {
            Assert.All(turns, x => Assert.True(x.Kind == OutcomeKind.Completed, $"{x.Kind} {x.Reason}: {x.Detail}"));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task AzureCandidateBecomingZeroSizeBeforeDeleteIsKept(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var pinned = await PinnedOrphanRun.SeedAsync(_kit, server);
            pinned.PutOld("a-zero.parquet", 3);
            pinned.PutOld("b-old.parquet", 3);
            // The head's live file
            pinned.PutOld("part-0002.parquet", 3);
            var run = await _kit.RegisterPinnedAsync(pinned);
            Assert.Equal(new[] { "a-zero.parquet", "b-old.parquet" }, OrphanSweepTests.TargetsOf(run, pinned.Table));
            var counters = pinned.Counters;
            var unknownSize = counters.OrphanKept(KeepReason.UnknownSize);
            // Emptied after the inventory froze it at three bytes
            server.Put("tables/t/a-zero.parquet", Array.Empty<byte>());
            server.Requests.Clear();

            var turns = await _kit.DeleteTurnsAsync(run, PinnedOrphanRun.Head);

            AssertCompleted(turns);
            Assert.True(server.Contains("tables/t/a-zero.parquet"));
            Assert.Contains("HEAD /c/tables/t/a-zero.parquet HTTP/1.1", server.Requests);
            // The run went on past the kept candidate
            Assert.Equal(new[] { "DELETE /c/tables/t/b-old.parquet HTTP/1.1" }, Deletes(server));
            Assert.False(server.Contains("tables/t/b-old.parquet"));
            Assert.True(server.Contains("tables/t/part-0002.parquet"));
            Assert.Equal(unknownSize + 1, counters.OrphanKept(KeepReason.UnknownSize));
            Assert.Equal(1, counters.Deletes(TargetKind.Payload, DeleteStatus.Kept));
            Assert.Equal(1, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
            // Both answered their submission probes
            Assert.Equal(2, counters.CandidateProbes(ProbeResult.Present));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task AzureCandidateProbeFailureKeepsObject(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var pinned = await PinnedOrphanRun.SeedAsync(_kit, server);
            pinned.PutOld("a-broken.parquet", 3);
            pinned.PutOld("b-old.parquet", 3);
            pinned.PutOld("part-0002.parquet", 3);
            var run = await _kit.RegisterPinnedAsync(pinned);
            Assert.Equal(new[] { "a-broken.parquet", "b-old.parquet" }, OrphanSweepTests.TargetsOf(run, pinned.Table));
            var counters = pinned.Counters;
            // Only that candidate's probe fails
            server.Intercept = (method, target) => method == "HEAD" && target == "/c/tables/t/a-broken.parquet" ? (500, "") : null;
            server.Requests.Clear();

            var turns = await _kit.DeleteTurnsAsync(run, PinnedOrphanRun.Head);

            AssertCompleted(turns);
            Assert.True(server.Contains("tables/t/a-broken.parquet"));
            Assert.Contains("HEAD /c/tables/t/a-broken.parquet HTTP/1.1", server.Requests);
            Assert.Equal(new[] { "DELETE /c/tables/t/b-old.parquet HTTP/1.1" }, Deletes(server));
            Assert.False(server.Contains("tables/t/b-old.parquet"));
            Assert.True(server.Contains("tables/t/part-0002.parquet"));
            Assert.Equal(1, counters.OrphanKept(KeepReason.ProbeError));
            Assert.Equal(1, counters.CandidateProbes(ProbeResult.Error));
            Assert.Equal(1, counters.CandidateProbes(ProbeResult.Present));
            Assert.Equal(1, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure, true)]
        [InlineData(StoreFlavor.Azure, false)]
        [InlineData(StoreFlavor.AzureHns, true)]
        [InlineData(StoreFlavor.AzureHns, false)]
        public async Task AzureRuntimeDirectoryExclusionAnomalyDeclinesWithoutThrowOrDelete(StoreFlavor flavor, bool first)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var pinned = await PinnedOrphanRun.SeedAsync(_kit, server);
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            foreach (var name in names)
            {
                pinned.PutOld(name, 3);
            }
            var run = await _kit.RegisterPinnedAsync(pinned);
            Assert.Equal(names, OrphanSweepTests.TargetsOf(run, pinned.Table));
            var folder = first ? names[0] : names[^1];
            // The submission probe reports a directory for a name frozen as a file of three bytes
            pinned.Table.Owned.StatAnswer = (path, entry) => path.Full == pinned.Table.Prefix + folder ? new IOEntry(path.Full + "/") { Size = 3 } : entry;
            var warned = new List<MaintenanceReason>();
            run.Warn = (reason, _) => warned.Add(reason);
            var directories = pinned.Counters.OrphanKept(KeepReason.Directory);
            server.Requests.Clear();

            var turns = await _kit.DeleteTurnsAsync(run, PinnedOrphanRun.Head);

            // The turn that met it retires the run
            var turn = Assert.Single(turns);
            Assert.True(turn.Kind == OutcomeKind.Declined && turn.Reason == MaintenanceReason.InvalidMaintenanceOptions, $"{turn.Kind} {turn.Reason}: {turn.Detail}");
            Assert.True(run.Ended);
            Assert.Equal(new[] { MaintenanceReason.InvalidMaintenanceOptions }, warned);
            Assert.Equal(directories + 1, pinned.Counters.OrphanKept(KeepReason.Directory));
            Assert.Contains($"HEAD /c/tables/t/{folder} HTTP/1.1", server.Requests);
            var deleted = first ? Array.Empty<string>() : names[..^1];
            Assert.Equal(deleted.Select(x => $"DELETE /c/tables/t/{x} HTTP/1.1"), Deletes(server));
            Assert.All(names.Except(deleted), x => Assert.True(server.Contains("tables/t/" + x), x));
            Assert.False(File.Exists(run.Targets.Path));
        }
    }
}
