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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaMaintenanceWorkerTests
    {
        [Fact]
        public void DeclineMemoStaysWithinItsBound()
        {
            var result = new BuildResult(BuildStatus.Declined, Reason: MaintenanceReason.NoAdmissibleRoot, Dependency: DeclineDependency.Root, Detail: new string('d', 100_000))
            {
                Candidates = Enumerable.Range(0, 2048).Select(version => new ObjectIdentity("/t", version, 123, s_now, "md5")).ToArray()
            };
            var memo = DeclineMemo.For(result, 2048, s_now)!;

            // A digest stands for the candidates however many there are, and the diagnostic is cut
            Assert.Equal(32, memo.CandidatesDigest.Length);
            Assert.Equal(DeclineMemo.MaxDetailLength, memo.Detail!.Length);
            Assert.Empty(memo.ProtocolCommits);
            Assert.False(memo.Weak);
        }

        [Fact]
        public async Task SuccessfulWorkerRetainsNoCheckpointHistory()
        {
            var rig = Create(interval: 1, stamp: true);
            const int checkpoints = 256;
            var root = OwnRootBytes(0);
            await Commits(rig.Storage, 0, 0);
            for (int version = 0; version < checkpoints; version++)
            {
                if (version != 0)
                {
                    await rig.Storage.WriteText($"/t/_delta_log/{version:D20}.json", "{\"commitInfo\":{\"timestamp\":0}}\n");
                }
                await WriteBytes(rig.Storage, $"/t/_delta_log/{version:D20}.checkpoint.parquet", root);
            }
            await rig.Storage.WriteText($"/t/_delta_log/{checkpoints:D20}.json", "{\"commitInfo\":{\"timestamp\":0}}\n");
            await WriteHint(rig.Storage, checkpoints - 1, root.Length);

            rig.Worker.Published(checkpoints);
            await rig.TickAsync();
            Assert.Equal(checkpoints, rig.Worker.LastCompleted);
            // Only fixed-size figures stay: no candidate lists through the last build or the test outcome
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Null(rig.Worker.LastOutcomeForTests.Build);
            Assert.Equal(checkpoints - 1, rig.Worker.LastBuild!.RootVersion);
            Assert.Null(rig.Worker.Memo);
        }

        [Fact]
        public async Task FailedTornInspectionRetainsNoCheckpointHistory()
        {
            var rig = Create(interval: 1, stamp: true);
            const int roots = 256;
            await Commits(rig.Storage, 0, 0);
            for (int version = 1; version <= roots + 1; version++)
            {
                await rig.Storage.WriteText($"/t/_delta_log/{version:D20}.json", "{\"commitInfo\":{\"timestamp\":0}}\n");
            }
            var root = OwnRootBytes(0);
            for (int version = 0; version < roots - 1; version++)
            {
                await WriteBytes(rig.Storage, CheckpointPath(version), root);
            }
            await SidecarCheckpoint(rig.Storage, roots - 1);
            await rig.Storage.WriteText(CheckpointPath(roots), "torn");
            rig.Worker.Published(roots + 1);
            await rig.TickAsync();

            var outcome = rig.Worker.LastOutcomeForTests!;
            Assert.Equal(OutcomeKind.Failed, outcome.Kind);
            Assert.Equal(MaintenanceReason.SidecarRoot, outcome.Reason);
            Assert.Single(rig.Worker.UnsupportedRoots);
            // The failed rebuild's exception carries its result, which lists every root
            Assert.Null(outcome.Build);
            Assert.Null(outcome.Error);
        }

        [Fact]
        public async Task DeclinedWorkerRetainsOnlyABoundedDiagnostic()
        {
            var rig = Create(interval: 1, stamp: true);
            const int roots = 512;
            await Commits(rig.Storage, 0, 0);
            for (int version = 1; version <= roots; version++)
            {
                await rig.Storage.WriteText($"/t/_delta_log/{version:D20}.json", "{\"commitInfo\":{\"timestamp\":0}}\n");
            }
            await DeltaLakeCheckpointBuildTests.OutOfEnvelopeRoot(rig.Storage, "t", 0);
            var root = await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(0));
            for (int version = 0; version < roots; version++)
            {
                await WriteBytes(rig.Storage, CheckpointPath(version), root);
            }
            rig.Worker.Published(roots);
            await rig.TickAsync();

            var outcome = rig.Worker.LastOutcomeForTests!;
            Assert.Equal(OutcomeKind.Declined, outcome.Kind);
            Assert.Equal(MaintenanceReason.NoAdmissibleRoot, outcome.Reason);
            Assert.Equal(DeclineMemo.MaxDetailLength, rig.Worker.Memo!.Detail!.Length);
            Assert.Equal(DeclineMemo.MaxDetailLength, outcome.Detail!.Length);
        }
    }
}
