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
using System.Text.Json;
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
            Assert.Equal(v0, ((JsonElement)create[0].CommitInfo!.Data!["timestamp"]).GetInt64());
            Assert.All(create.Where(x => x.Add != null), x => Assert.Equal(v0, x.Add!.ModificationTime));

            for (long version = 1; version <= 2; version++)
            {
                var expected = Start.AddHours(version).ToUnixTimeMilliseconds();
                var actions = await ReadCommitActions(storage, "test", version);
                Assert.Equal(expected, ((JsonElement)actions[0].CommitInfo!.Data!["timestamp"]).GetInt64());
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

                using var line = JsonDocument.Parse(text.Split('\n')[0]);
                var keys = line.RootElement.GetProperty("commitInfo").EnumerateObject().Select(x => x.Name).ToList();
                if (version == 1)
                {
                    Assert.Equal(new[] { "flowtide.stageId", "timestamp", "operation" }, keys);
                }
                else
                {
                    Assert.Equal(new[] { "flowtide.stageId", "timestamp" }, keys.Take(2));
                    Assert.Single(keys, x => x.StartsWith("flowtide."));
                }
            }
            Assert.Equal(2, stageIds.Count);
        }

        [Fact]
        public async Task ACommitInfoWithoutAStageIdWritesNoFlowtideKey()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ACommitInfoWithoutAStageIdWritesNoFlowtideKey)}");
            await WriteCommit(storage, "test", 0, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "WRITE" } } }, Protocol(), Metadata(UserSchema));

            // An unset stage id is left out, not written as null; the metadata line spells nullable
            var text = Encoding.UTF8.GetString(await ReadBytes(storage, CommitPath("test", 0)));
            Assert.DoesNotContain("flowtide", text);
            Assert.DoesNotContain("null", text.Split('\n')[0]);
        }
    }
}
