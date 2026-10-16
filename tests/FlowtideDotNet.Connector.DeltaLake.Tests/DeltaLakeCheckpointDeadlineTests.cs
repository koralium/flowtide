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
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeCheckpointBuildTests
    {
        [Fact]
        public async Task LongDeadlineStopsFallbackRootDownloads()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory("./deadline-roots-" + Guid.NewGuid().ToString("N")));
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 9);
            await LegacyRoot(storage, "t", 1);
            await LegacyRoot(storage, "t", 3);
            await storage.WriteText($"/t/_delta_log/{3:D20}.checkpoint.parquet", "torn");

            var clock = new FakeTimeProvider(s_now);
            var deadline = clock.GetUtcNow() + TimeSpan.FromHours(6);
            var roots = new List<string>();
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name.EndsWith(".checkpoint.parquet"))
                {
                    roots.Add(path.Name);
                    if (path.Name == $"{3:D20}.checkpoint.parquet")
                    {
                        clock.Advance(TimeSpan.FromHours(6));
                    }
                }
                return Task.CompletedTask;
            };
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            using var scratch = owner.CreateJob();
            var context = new CheckpointBuildContext()
            {
                Storage = storage,
                Table = "t",
                Scratch = scratch,
                Memory = new JobMemory(),
                Clock = clock,
                Publish = false
            };

            var result = await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = 9, Deadline = deadline });

            Assert.Equal(BuildStatus.Declined, result.Status);
            Assert.Equal(MaintenanceReason.JobDeadline, result.Reason);
            Assert.Equal(new[] { $"{3:D20}.checkpoint.parquet" }, roots);
        }

        [Fact]
        public async Task LongDeadlineStopsExistingCheckpointHintRepair()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory("./deadline-hint-" + Guid.NewGuid().ToString("N")));
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 9);
            await LegacyRoot(storage, "t", 9);
            var clock = new FakeTimeProvider(s_now);
            var deadline = clock.GetUtcNow() + TimeSpan.FromHours(6);
            var writes = new List<string>();
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name == $"{9:D20}.checkpoint.parquet")
                {
                    clock.Advance(TimeSpan.FromHours(6));
                }
                if (verb == "OpenWrite")
                {
                    writes.Add(path.Name);
                }
                return Task.CompletedTask;
            };
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            using var scratch = owner.CreateJob();
            var context = new CheckpointBuildContext()
            {
                Storage = storage,
                Table = "t",
                Scratch = scratch,
                Memory = new JobMemory(),
                Clock = clock
            };

            var result = await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = 9, Deadline = deadline });

            Assert.Empty(writes);
            Assert.Equal(BuildStatus.Declined, result.Status);
            Assert.Equal(MaintenanceReason.JobDeadline, result.Reason);
        }
    }
}
