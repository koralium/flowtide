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
using Stowage;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CommitHeaderCompatibilityTests
    {
        [Theory]
        [InlineData(100)]
        [InlineData(70_000)]
        public async Task StagePrefixedForeignHeaderPreservesExtensionData(int paddingLength)
        {
            var storage = Files.Of.InternalMemory("./" + nameof(StagePrefixedForeignHeaderPreservesExtensionData) + paddingLength);
            var line = "{\"commitInfo\":{\"flowtide.stageId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"timestamp\":100,\"operation\":\"WRITE\",\"engineInfo\":\"foreign-writer\",\"padding\":\"" + new string('x', paddingLength) + "\"}}\n";
            await storage.WriteText("/t/_delta_log/00000000000000000000.json", line);

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0);

            Assert.NotNull(commit);
            Assert.NotNull(commit.CommitInfo);
            Assert.NotNull(commit.CommitInfo.Data);
            Assert.Equal("foreign-writer", ((JsonElement)commit.CommitInfo.Data["engineInfo"]).GetString());
        }

        [Theory]
        [InlineData(100)]
        [InlineData(70_000)]
        public async Task StagePrefixedHeaderPreservesOperationAcrossStreamingThreshold(int paddingLength)
        {
            var storage = Files.Of.InternalMemory("./" + nameof(StagePrefixedHeaderPreservesOperationAcrossStreamingThreshold) + paddingLength);
            var operation = new string('w', 1025);
            var line = "{\"commitInfo\":{\"flowtide.stageId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"timestamp\":100,\"operation\":\"" + operation + "\",\"padding\":\"" + new string('x', paddingLength) + "\"}}\n";
            await storage.WriteText("/t/_delta_log/00000000000000000000.json", line);

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0);

            Assert.NotNull(commit);
            Assert.Equal(operation, commit.CommitInfo!.Operation);
        }

        // A protocol and metaData after the given first line
        private static async Task<IFileStorage> TableWithFirstLine(string name, string firstLine)
        {
            var storage = Files.Of.InternalMemory("./" + name);
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            var rest = await storage.ReadText("/t/_delta_log/00000000000000000000.json");
            await storage.WriteText("/t/_delta_log/00000000000000000000.json", firstLine + "\n" + rest);
            return storage;
        }

        [Theory]
        [InlineData(100)]
        [InlineData(70_000)]
        public async Task OwnLineWithASecondMemberReadsAlikeAcrossStreamingThreshold(int paddingLength)
        {
            var line = "{\"commitInfo\":{\"flowtide.stageId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"flowtide.adoptedAt\":0,\"padding\":\"" + new string('x', paddingLength) + "\"},\"add\":{\"path\":\"a.parquet\",\"partitionValues\":{},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}";
            var storage = await TableWithFirstLine(nameof(OwnLineWithASecondMemberReadsAlikeAcrossStreamingThreshold) + paddingLength, line);

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0);
            var table = await DeltaTransactionReader.ReadTable(storage, "t");

            Assert.Equal("a.parquet", Assert.Single(commit!.AddedFiles).Path);
            Assert.Equal("a.parquet", Assert.Single(table!.AddFiles).Path);
            Assert.False(table.HeadHeader!.IsCommitInfo);
        }

        [Theory]
        [InlineData(100)]
        [InlineData(70_000)]
        public async Task TornOwnLineFailsAlikeAcrossStreamingThreshold(int paddingLength)
        {
            var line = "{\"commitInfo\":{\"flowtide.stageId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"padding\":\"" + new string('x', paddingLength) + "\"";
            var storage = await TableWithFirstLine(nameof(TornOwnLineFailsAlikeAcrossStreamingThreshold) + paddingLength, line);

            await Assert.ThrowsAnyAsync<JsonException>(() => DeltaTransactionReader.ReadVersionCommit(storage, "t", 0));
            await Assert.ThrowsAnyAsync<JsonException>(() => DeltaTransactionReader.ReadTable(storage, "t"));
        }
    }
}
