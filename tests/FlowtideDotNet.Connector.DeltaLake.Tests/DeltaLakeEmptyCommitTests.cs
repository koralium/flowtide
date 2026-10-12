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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeEmptyCommitTests
    {
        [Fact]
        public async Task IdleCheckpointsWriteNoVersion()
        {
            var testName = nameof(IdleCheckpointsWriteNoVersion);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            await RunCheckpoints(stream, 5);

            Assert.False(await storage.Exists(CommitPath("test", 1)));
            Assert.Empty(await DeltaTransactionReader.ListHiddenLogFiles(storage, "test"));
            await AssertNextDataIsVersion(storage, stream, 1);
        }

        [Fact]
        public async Task InsertAndDeleteInOneCheckpointWriteNoVersion()
        {
            var testName = nameof(InsertAndDeleteInOneCheckpointWriteNoVersion);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            stream.EnterDataWriteLock();
            var user = new User() { UserKey = 1_000_000, FirstName = "temporary" };
            stream.AddOrUpdateUser(user);
            stream.DeleteUser(user);
            stream.ExitDataWriteLock();
            await RunCheckpoints(stream, 3);

            Assert.False(await storage.Exists(CommitPath("test", 1)));
            await AssertNextDataIsVersion(storage, stream, 1);
        }

        [Fact]
        public async Task OverwriteOfAnEmptyTableWithoutRowsWritesNoVersion()
        {
            var testName = nameof(OverwriteOfAnEmptyTableWithoutRowsWritesNoVersion);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(@"
                INSERT OVERWRITE test
                SELECT userKey AS userkey, firstName AS name FROM users WHERE userKey < 0
            ");

            await RunCheckpoints(stream, 3);

            Assert.False(await storage.Exists(CommitPath("test", 1)));
            Assert.Equal(0, (await DeltaTransactionReader.ReadTable(storage, "test"))!.Version);
        }

        // No empty version was written before, the next change takes the next version
        private static async Task AssertNextDataIsVersion(IFileStorage storage, DeltaLakeSinkStream stream, long version)
        {
            stream.Generate(1);
            await WaitForVersionCheckpointing(storage, "test", stream, version);
            Assert.Contains(await ReadCommitActions(storage, "test", version), x => x.Add != null);
        }

        [Fact]
        public async Task OverwriteWithoutRowsStillRemovesTheOldFiles()
        {
            var testName = nameof(OverwriteWithoutRowsStillRemovesTheOldFiles);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema), new DeltaAction() { Add = new DeltaAddAction() { Path = "old.parquet", Size = 1, DataChange = true, PartitionValues = new Dictionary<string, string>() } });
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(@"
                INSERT OVERWRITE test
                SELECT userKey AS userkey, firstName AS name FROM users WHERE userKey < 0
            ");

            await WaitForVersionCheckpointing(storage, "test", stream, 1);

            var actions = await ReadCommitActions(storage, "test", 1);
            Assert.Equal("old.parquet", Assert.Single(actions, x => x.Remove != null).Remove!.Path);
            Assert.Empty((await DeltaTransactionReader.ReadTable(storage, "test"))!.AddFiles);
        }

        [Fact]
        public void ChangeDataAloneIsAChange()
        {
            var commitInfo = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() };
            var cdc = new DeltaAction() { Cdc = new DeltaCdcAction() { Path = "_change_data/x.parquet" } };

            Assert.False(DeltaLakeSink.HasChanges(new List<DeltaAction>() { commitInfo }));
            Assert.True(DeltaLakeSink.HasChanges(new List<DeltaAction>() { commitInfo, cdc }));
        }
    }
}
