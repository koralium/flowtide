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
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class BlockedContextTests : IDisposable
    {
        private readonly string _root = Path.Join(Path.GetTempPath(), "blocked-" + ScratchOwners.ShortId());

        public BlockedContextTests()
        {
            Directory.CreateDirectory(_root);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_root, true);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                // A folder the system still holds is left in temp
            }
        }

        // Runs nothing posted to it, like a blocked single thread
        private sealed class UnpumpedContext : SynchronizationContext
        {
            public override void Post(SendOrPostCallback d, object? state)
            {
            }
        }

        [Fact]
        public async Task ATableLookupDoesNotWaitOnTheCallersContext()
        {
            var storage = Files.Of.LocalDisk(_root);
            await WriteCommit(storage, "t", 0, Protocol(), Metadata(UserSchema));
            var provider = new DeltaLakeSqlProvider(new DeltaLakeOptions() { StorageLocation = storage });
            var found = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var thread = new Thread(() =>
            {
                SynchronizationContext.SetSynchronizationContext(new UnpumpedContext());
                try
                {
                    found.TrySetResult(provider.TryGetTableInformation(new[] { "t" }, out _));
                }
                catch (Exception e)
                {
                    found.TrySetException(e);
                }
            })
            {
                IsBackground = true
            };

            thread.Start();

            Assert.True(await found.Task.WaitAsync(TimeSpan.FromSeconds(30)));
        }

        [Fact]
        public async Task ACommitClosesItsFileWithoutBlocking()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(ACommitClosesItsFileWithoutBlocking)}")) { RecordSyncDisposes = true };

            await DeltaTransactionWriter.WriteCommit(storage, "t", 0, new List<DeltaAction>() { Protocol(), Metadata(UserSchema) });
            await DeltaTransactionWriter.StageCommit(storage, "t", 1, new List<DeltaAction>() { Protocol(), Metadata(UserSchema) });

            Assert.Equal(2, storage.Requests.Count(x => x.StartsWith("OpenWrite ")));
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("SyncDispose "));
        }
    }
}
