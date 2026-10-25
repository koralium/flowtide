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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CheckpointOracleTests
    {
        [Theory]
        [InlineData("all_primitive_types")]
        [InlineData("nested_types")]
        [InlineData("simple_table_with_cdc")]
        [InlineData("table_with_column_mapping")]
        [InlineData("table_with_deletion_logs")]
        public async Task OracleMatchesReadTableOnGoldenTables(string name)
        {
            var storage = Files.Of.LocalDisk("../../../testdata");
            var table = (await DeltaTransactionReader.ReadTable(storage, name, options: new DeltaReadOptions() { SkipTombstones = false }))!;

            // Tombstones on and expiry off, as the reader keeps them
            var expected = await CheckpointOracle.FullHistory(storage, name, table.Version, DateTimeOffset.UnixEpoch, expire: false);
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { Protocol = table.Protocol },
                new DeltaAction() { MetaData = table.Metadata }
            };
            actions.AddRange(table.AddFiles.Select(x => new DeltaAction() { Add = x }));
            actions.AddRange(table.Tombstones.Select(x => new DeltaAction() { Remove = x }));
            actions.AddRange(table.Transactions.Values.Select(x => new DeltaAction() { Txn = x }));
            actions.AddRange(table.DomainMetadata.Values.Where(x => !x.Removed).Select(x => new DeltaAction() { DomainMetadata = x }));

            Assert.Contains(expected, x => x.StartsWith("{\"add\":"));
            Assert.Contains(expected, x => x.StartsWith("{\"metaData\":"));
            if (name == "table_with_deletion_logs")
            {
                Assert.Contains(expected, x => x.StartsWith("{\"remove\":"));
            }
            Assert.Equal(expected, CheckpointOracle.Canonical(actions));
        }

        [Fact]
        public async Task OracleExpiresByExactTicks()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(OracleExpiresByExactTicks)}");
            var configuration = new Dictionary<string, string>() { ["delta.deletedFileRetentionDuration"] = "interval 1500 microseconds" };
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, configuration),
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "at.parquet", DeletionTimestamp = 1_000 } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "below.parquet", DeletionTimestamp = 999 } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "none.parquet" } });

            // now = 1001.5 ms, so the cutoff is 1000.0 ms: a tombstone at the cutoff stays, one below it expires
            var now = DateTimeOffset.UnixEpoch.AddTicks(1_001_5000);
            var lines = await CheckpointOracle.FullHistory(storage, "t", 0, now);

            Assert.Contains(lines, x => x.Contains("\"at.parquet\""));
            Assert.DoesNotContain(lines, x => x.Contains("\"below.parquet\""));
            Assert.DoesNotContain(lines, x => x.Contains("\"none.parquet\""));
        }

        [Fact]
        public async Task AbandonedStreamCommitsNothing()
        {
            var storage = new CommitOnDisposeStorage(Files.Of.InternalMemory($"./{nameof(AbandonedStreamCommitsNothing)}"));
            var stream = await storage.OpenWrite("/t/a.bin");
            await stream.WriteAsync(new byte[] { 1, 2, 3 });

            Assert.False(await storage.Exists("/t/a.bin"));
            GC.KeepAlive(stream);
        }

        [Fact]
        public async Task DisposedAfterFaultCommitsPrefix()
        {
            var storage = new CommitOnDisposeStorage(Files.Of.InternalMemory($"./{nameof(DisposedAfterFaultCommitsPrefix)}"));
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var stream = await storage.OpenWrite("/t/a.bin");
                await stream.WriteAsync(new byte[] { 1, 2, 3 });
                throw new InvalidOperationException("fault mid-write");
            });

            Assert.Equal(new byte[] { 1, 2, 3 }, await DeltaTestKit.ReadBytes(storage, "/t/a.bin"));
        }
    }
}
