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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Text;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CommitInfoTests
    {
        private static readonly DateTimeOffset Start = new DateTimeOffset(2031, 5, 6, 7, 8, 9, TimeSpan.Zero);

        // A clock tests may move backwards
        private sealed class ManualClock : TimeProvider
        {
            public DateTimeOffset Now { get; set; } = Start;

            public override DateTimeOffset GetUtcNow() => Now;
        }

        [Fact]
        public async Task CommitInfoFieldOrder()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(CommitInfoFieldOrder)}");
            await using var stream = new DeltaLakeSinkStream(nameof(CommitInfoFieldOrder), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 1);

            foreach (var version in new[] { 0L, 1L })
            {
                var line = Encoding.UTF8.GetString(await ReadBytes(storage, CommitPath("test", version))).Split('\n')[0];
                using var document = JsonDocument.Parse(line);
                var commitInfo = document.RootElement.GetProperty("commitInfo");
                Assert.Equal(new[] { "flowtide.stageId", "flowtide.adoptedAt", "timestamp", "operation", "flowtide.createdFiles" }, commitInfo.EnumerateObject().Select(x => x.Name));
                // Own writes carry no extension data
                Assert.Null((await ReadCommitActions(storage, "test", version))[0].CommitInfo!.Data);
                Assert.Equal(version == 0 ? "CREATE TABLE" : "WRITE", commitInfo.GetProperty("operation").GetString());
            }
        }

        [Fact]
        public async Task StagePrefixIsUnchanged()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(StagePrefixIsUnchanged)}");
            await using var stream = new DeltaLakeSinkStream(nameof(StagePrefixIsUnchanged), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            var bytes = await ReadBytes(storage, CommitPath("test", 0));
            var stageId = (await ReadCommitActions(storage, "test", 0))[0].CommitInfo!.StageId!;
            Assert.Matches("^[0-9a-f]{32}$", stageId);
            var prefix = DeltaTransactionWriter.StagePrefix(stageId);
            Assert.Equal(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"" + stageId + "\""), prefix);
            Assert.True(bytes.AsSpan().StartsWith(prefix));
            Assert.True(bytes.AsSpan().StartsWith(CommitHeaderReader.OwnPrefix));
        }

        [Fact]
        public async Task TimestampsAreMonotonicUnderBackwardClock()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(TimestampsAreMonotonicUnderBackwardClock)}");
            var clock = new ManualClock();
            await using var stream = new DeltaLakeSinkStream(nameof(TimestampsAreMonotonicUnderBackwardClock), storage, options => options.TimeProvider = clock);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            var t0 = Start.ToUnixTimeMilliseconds();

            clock.Now = Start.AddHours(-1);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 1);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 2);
            clock.Now = Start.AddHours(1);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 3);

            var timestamps = new List<long>();
            for (long version = 0; version <= 3; version++)
            {
                timestamps.Add((await ReadCommitActions(storage, "test", version))[0].CommitInfo!.Timestamp!.Value);
            }
            Assert.Equal(new[] { t0, t0 + 1, t0 + 2, Start.AddHours(1).ToUnixTimeMilliseconds() }, timestamps);
            // Files and removes keep the clock's time
            Assert.All((await ReadCommitActions(storage, "test", 1)).Where(x => x.Add != null), x => Assert.Equal(Start.AddHours(-1).ToUnixTimeMilliseconds(), x.Add!.ModificationTime));
        }

        [Fact]
        public async Task TimestampOverflowSaturatesWithoutFailingIngestion()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(TimestampOverflowSaturatesWithoutFailingIngestion)}");
            await WriteCommit(storage, "test", 0, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = new string('c', 32), AdoptedAt = 0, Timestamp = long.MaxValue, Operation = "CREATE TABLE" } }, Protocol(), Metadata(UserSchema));

            await using var stream = new DeltaLakeSinkStream(nameof(TimestampOverflowSaturatesWithoutFailingIngestion), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);
            stream.Generate(1);
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(long.MaxValue, (await ReadCommitActions(storage, "test", 1))[0].CommitInfo!.Timestamp);
            Assert.Equal(long.MaxValue, (await ReadCommitActions(storage, "test", 2))[0].CommitInfo!.Timestamp);
            Assert.Equal(long.MaxValue, CommitProvenance.NextTimestamp(5, new CommitProvenance(0, "s", 0, long.MaxValue)));
            Assert.Equal(5, CommitProvenance.NextTimestamp(5, new CommitProvenance(0, "s", 0, null)));
            Assert.Equal(5, CommitProvenance.NextTimestamp(5, null));
        }
    }
}
