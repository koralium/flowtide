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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using Xunit;

namespace FlowtideDotNet.Storage.Tests
{
    public partial class BackgroundCommitTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task NewEmptyQueueCanRecoverAfterCommit(bool backgroundCommit)
        {
            var (manager, storage) = await CreateManager($"new_empty_queue_{backgroundCommit}", backgroundCommit: backgroundCommit, reservoir: true);
            using (storage)
            using (manager)
            {
                var queue = await CreateQueue(manager);
                await queue.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                await manager.InitializeAsync().WaitAsync(Timeout);
                var recovered = await CreateQueue(manager).AsTask().WaitAsync(Timeout);
                Assert.Equal(0, recovered.Count);
                await recovered.Enqueue(2);
                Assert.Equal(2, await recovered.Dequeue());
            }
        }

        [Fact]
        [Trait("Category", "BackgroundCommitRegression")]
        public async Task EmptyQueueCanRecoverAfterClearAndCommit()
        {
            var (manager, storage) = await CreateManager("queue_clear_checkpoint", reservoir: true);
            using (storage)
            using (manager)
            {
                var queue = await CreateQueue(manager);
                await queue.Enqueue(1);
                await queue.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                // No enqueue follows Clear, so the empty replacement root itself must be dirty.
                await queue.Clear().AsTask().WaitAsync(Timeout);
                await queue.Commit().AsTask().WaitAsync(Timeout);
                await manager.CheckpointAsync().AsTask().WaitAsync(Timeout);

                await manager.InitializeAsync().WaitAsync(Timeout);
                var recovered = await CreateQueue(manager).AsTask().WaitAsync(Timeout);
                Assert.Equal(0, recovered.Count);
                await recovered.Enqueue(2);
                Assert.Equal(2, await recovered.Dequeue());
            }
        }

    }
}
