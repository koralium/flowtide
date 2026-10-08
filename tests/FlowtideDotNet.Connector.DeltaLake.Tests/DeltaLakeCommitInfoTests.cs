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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text;
using System.Text.RegularExpressions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeCommitInfoTests
    {
        private static readonly DateTimeOffset Start = new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.Zero);

        [Fact]
        public async Task EveryTimestampComesFromTheTimeProvider()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(EveryTimestampComesFromTheTimeProvider)}");
            var clock = new FakeTimeProvider(Start);
            await using var stream = new DeltaLakeSinkStream(nameof(EveryTimestampComesFromTheTimeProvider), storage, options => options.TimeProvider = clock);
            stream.Generate(20);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            clock.Advance(TimeSpan.FromHours(1));
            // One of 20 rows writes a deletion vector, two rewrite the file
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);
            clock.Advance(TimeSpan.FromHours(1));
            stream.DeleteUser(stream.Users[0]);
            stream.DeleteUser(stream.Users[1]);
            await WaitForVersion(storage, "test", stream, 2);

            var create = await ReadCommitActions(storage, "test", 0);
            var v0 = Start.ToUnixTimeMilliseconds();
            Assert.Equal(v0, create.Single(x => x.MetaData != null).MetaData!.CreatedTime);
            Assert.Equal(v0, create[0].CommitInfo!.Timestamp);
            Assert.All(create.Where(x => x.Add != null), x => Assert.Equal(v0, x.Add!.ModificationTime));

            for (long version = 1; version <= 2; version++)
            {
                var expected = Start.AddHours(version).ToUnixTimeMilliseconds();
                var actions = await ReadCommitActions(storage, "test", version);
                Assert.Equal(expected, actions[0].CommitInfo!.Timestamp);
                Assert.NotEmpty(actions.Where(x => x.Remove != null));
                Assert.All(actions.Where(x => x.Remove != null), x => Assert.Equal(expected, x.Remove!.DeletionTimestamp));
            }
            // A rewrite adds new files at the commit time, a deletion vector keeps the time of the file
            var rewrite = await ReadCommitActions(storage, "test", 2);
            Assert.All(rewrite.Where(x => x.Add != null), x => Assert.Equal(Start.AddHours(2).ToUnixTimeMilliseconds(), x.Add!.ModificationTime));
        }

        [Fact]
        public async Task CommitInfoIsTheFirstLineAndStartsWithTheStageId()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CommitInfoIsTheFirstLineAndStartsWithTheStageId)}");
            await using var stream = new DeltaLakeSinkStream(nameof(CommitInfoIsTheFirstLineAndStartsWithTheStageId), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 1);

            var stageIds = new HashSet<string>();
            foreach (var version in new[] { 0L, 1L })
            {
                var text = Encoding.UTF8.GetString(await ReadBytes(storage, CommitPath("test", version)));
                var match = Regex.Match(text, "^\\{\"commitInfo\":\\{\"flowtide.stageId\":\"([0-9a-f]{32})\"");
                Assert.True(match.Success, text.Split('\n')[0]);
                stageIds.Add(match.Groups[1].Value);
                var actions = await ReadCommitActions(storage, "test", version);
                Assert.Equal(match.Groups[1].Value, actions[0].CommitInfo!.StageId);
                Assert.Single(actions, x => x.CommitInfo != null);
            }
            Assert.Equal(2, stageIds.Count);
        }

        [Fact]
        public async Task CreatedFilesListsEveryNewFile()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CreatedFilesListsEveryNewFile)}");
            await using var stream = new DeltaLakeSinkStream(nameof(CreatedFilesListsEveryNewFile), storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Rolls the inline rewrite into several files
                options.MaxFileSizeBytes = 64;
            });
            stream.Generate(40);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // Insert only, change data files are removed again
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 1);
            // Deletes keep change data and rewrite or mark the files
            stream.DeleteUser(stream.Users[0]);
            stream.DeleteUser(stream.Users[1]);
            await WaitForVersion(storage, "test", stream, 2);

            for (long version = 0; version <= 2; version++)
            {
                var actions = await ReadCommitActions(storage, "test", version);
                var created = actions[0].CommitInfo!.CreatedFiles;
                Assert.NotNull(created);
                var expected = new HashSet<string>();
                foreach (var action in actions)
                {
                    if (action.Add != null && action.Add.DeletionVector == null)
                    {
                        expected.Add(action.Add.Path!);
                    }
                    if (action.Cdc != null)
                    {
                        expected.Add(action.Cdc.Path!);
                    }
                }
                // Deletion vector containers are new, the re-added data file is not
                var reAdded = actions.Where(x => x.Add?.DeletionVector != null).Select(x => x.Add!.Path!).ToHashSet();
                Assert.All(created, path => Assert.DoesNotContain(path, reAdded));
                var vectors = created.Where(x => x.EndsWith(".bin")).ToList();
                Assert.Equal(reAdded.Count, vectors.Count);
                Assert.Equal(expected.OrderBy(x => x), created.Where(x => !x.EndsWith(".bin")).OrderBy(x => x));
                foreach (var path in created)
                {
                    Assert.True(await storage.Exists($"/test/{path}"), path);
                }
                if (version == 1)
                {
                    Assert.DoesNotContain(created, x => x.StartsWith("_change_data/"));
                }
                if (version == 2)
                {
                    Assert.Contains(created, x => x.StartsWith("_change_data/"));
                }
            }
        }

        [Fact]
        public async Task CreatedFilesListsANewDeletionVectorButNotTheReAddedFile()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CreatedFilesListsANewDeletionVectorButNotTheReAddedFile)}");
            await using var stream = new DeltaLakeSinkStream(nameof(CreatedFilesListsANewDeletionVectorButNotTheReAddedFile), storage);
            stream.Generate(40);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // One of 40 rows is below the rewrite limit, a deletion vector is written
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);

            var actions = await ReadCommitActions(storage, "test", 1);
            var reAdd = Assert.Single(actions, x => x.Add != null).Add!;
            Assert.NotNull(reAdd.DeletionVector);
            var created = actions[0].CommitInfo!.CreatedFiles!;
            Assert.Equal(new[] { reAdd.DeletionVector.GetAbsolutePath() }, created);
            Assert.DoesNotContain(reAdd.Path, created);
            Assert.True(await storage.Exists($"/test/{created[0]}"));
        }

        [Fact]
        public async Task AdoptionStartsAtZeroForANewTableAndIsInherited()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(AdoptionStartsAtZeroForANewTableAndIsInherited)}");
            await using var stream = new DeltaLakeSinkStream(nameof(AdoptionStartsAtZeroForANewTableAndIsInherited), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 1);

            Assert.Equal(0, (await ReadCommitActions(storage, "test", 0))[0].CommitInfo!.AdoptedAt);
            Assert.Equal(0, (await ReadCommitActions(storage, "test", 1))[0].CommitInfo!.AdoptedAt);
        }

        [Theory]
        [InlineData(0, false)]
        [InlineData(1, true)]
        public async Task AForeignOrLegacyHeadStartsANewEpoch(int id, bool legacyFlowtideHead)
        {
            var testName = $"{nameof(AForeignOrLegacyHeadStartsANewEpoch)}_{id}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            var head = new List<DeltaAction>();
            if (legacyFlowtideHead)
            {
                // What the sink wrote before stage ids existed
                head.Add(new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "WRITE" } } });
            }
            head.Add(Protocol());
            head.Add(Metadata(UserSchema));
            await WriteCommit(storage, "test", 0, head.ToArray());
            // Unset typed fields are left out, not written as null
            Assert.DoesNotContain("flowtide", System.Text.Encoding.UTF8.GetString(await ReadBytes(storage, CommitPath("test", 0))));

            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(1, (await ReadCommitActions(storage, "test", 1))[0].CommitInfo!.AdoptedAt);
            Assert.Equal(1, (await ReadCommitActions(storage, "test", 2))[0].CommitInfo!.AdoptedAt);
        }
    }
}
