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
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ChangeDataTests
    {
        // Fixed rows, the generator's seed is shared by every test
        private static readonly List<User> Users = Enumerable.Range(1, 12).Select(x => new User() { UserKey = x, FirstName = $"First{x}" }).ToList();

        [Fact]
        public async Task AnInsertOnlyCheckpointWritesNoChangeData()
        {
            var testName = nameof(AnInsertOnlyCheckpointWritesNoChangeData);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // A file per row, so change rows fill files mid-walk
                options.MaxFileSizeBytes = 1;
            });
            stream.AddUsers(Users.Take(10).ToList());
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.AddUsers(Users.Skip(10).ToList());
            await WaitForVersion(storage, "test", stream, 1);

            // Readers take the inserts from the add actions
            for (long version = 0; version <= 1; version++)
            {
                var actions = await ReadCommitActions(storage, "test", version);
                Assert.Contains(actions, x => x.Add != null);
                Assert.DoesNotContain(actions, x => x.Cdc != null);
            }
            Assert.DoesNotContain(storage.Requests, x => x.Contains("_change_data"));
        }

        [Fact]
        public async Task AMixedCheckpointStillWritesItsInserts()
        {
            var testName = nameof(AMixedCheckpointStillWritesItsInserts);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.WriteChangeDataOnNewTables = true);
            stream.AddUsers(Users.Take(10).ToList());
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // One source fetch sees both changes
            await stream.EnterDataWriteLockAsync();
            try
            {
                stream.AddUsers(Users.Skip(10).ToList());
                stream.DeleteUsers(Users.Take(1).ToList());
            }
            finally
            {
                stream.ExitDataWriteLock();
            }
            await WaitForVersion(storage, "test", stream, 1);

            Assert.Equal(new[] { "delete 1", "insert 11", "insert 12" }, await ChangeKeys(storage, "test", 1));
        }

        [Fact]
        public async Task AnInsertOnlyCheckpointAfterADeleteWritesNoChangeData()
        {
            var testName = nameof(AnInsertOnlyCheckpointAfterADeleteWritesNoChangeData);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // A file per row, so change rows fill files mid-walk
                options.MaxFileSizeBytes = 1;
            });
            stream.AddUsers(Users.Take(10).ToList());
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // The delete's commit clears the tree and the flag
            stream.DeleteUsers(Users.Take(1).ToList());
            await WaitForVersion(storage, "test", stream, 1);
            Assert.Equal(new[] { "delete 1" }, await ChangeKeys(storage, "test", 1));

            storage.ClearRequests();
            stream.AddUsers(Users.Skip(10).ToList());
            await WaitForVersion(storage, "test", stream, 2);

            Assert.DoesNotContain(await ReadCommitActions(storage, "test", 2), x => x.Cdc != null);
            Assert.DoesNotContain(storage.Requests, x => x.Contains("_change_data"));
        }
    }
}
