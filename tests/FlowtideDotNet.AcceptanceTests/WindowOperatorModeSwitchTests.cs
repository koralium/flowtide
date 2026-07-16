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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;

namespace FlowtideDotNet.AcceptanceTests
{
    /// <summary>
    /// Tests that switching between the bulk and non bulk window operator on existing state fails loudly.
    /// </summary>
    public class WindowOperatorModeSwitchTests
    {
        /// <summary>
        /// Keeps the stored files alive when the storage is disposed, so state survives a restart.
        /// </summary>
        private sealed class KeepAliveMemoryFileProvider : MemoryFileProvider, Storage.Persistence.Reservoir.IReservoirStorageProvider
        {
            public new void Dispose()
            {
            }
        }

        /// <summary>
        /// Test stream that uses a shared file provider, so two streams can share persisted state.
        /// </summary>
        private sealed class SharedStorageTestStream : FlowtideTestStream
        {
            private readonly KeepAliveMemoryFileProvider _fileProvider;

            public SharedStorageTestStream(string testName, KeepAliveMemoryFileProvider fileProvider) : base(testName)
            {
                _fileProvider = fileProvider;
            }

            protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
            {
                return new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = _fileProvider });
            }
        }

        private const string RunningSumQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users";

        private static void SeedUsers(FlowtideTestStream stream)
        {
            for (int i = 0; i < 10; i++)
            {
                stream.AddOrUpdateUser(new User()
                {
                    UserKey = i,
                    CompanyId = "1",
                    DoubleValue = i + 1
                });
            }
        }

        [Fact]
        public async Task RowModeRestartOverBulkWindowStateFailsLoudly()
        {
            // The bulk operator stores its state under other names than the non bulk operator.
            // A row mode restart on a bulk checkpoint must fail instead of restoring an empty tree.
            var fileProvider = new KeepAliveMemoryFileProvider();

            // Run in column store mode and stop, the checkpoint holds bulk window state.
            await using (var columnStream = new SharedStorageTestStream("WindowModeSwitch/RowModeRestart", fileProvider))
            {
                SeedUsers(columnStream);
                await columnStream.StartStream(RunningSumQuery);
                await columnStream.WaitForUpdate();
                await columnStream.StopStream();
            }

            // Restart in row mode on the same state, with the same plan so the plan hash matches.
            await using var rowStream = new SharedStorageTestStream("WindowModeSwitch/RowModeRestart", fileProvider);
            rowStream.UseColumnStore = false;
            SeedUsers(rowStream);

            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await rowStream.StartStream(RunningSumQuery);

                // Push a change through the restored stream, surfacing any failure from the restore.
                rowStream.AddOrUpdateUser(new User()
                {
                    UserKey = 100,
                    CompanyId = "1",
                    DoubleValue = 100
                });
                await rowStream.WaitForUpdate();
            });
            Assert.Contains("Reset the stream state", exception.Message);
        }
    }
}
