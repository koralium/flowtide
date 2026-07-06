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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    /// <summary>
    /// Tests for the stop and pause gates on the ingress output. Data must never be
    /// delivered after a gate was set, the stop gate in particular is set together with
    /// the final stop barrier and anything delivered after it mutates state the barrier
    /// already covered.
    /// </summary>
    public class IngressOutputGateTests
    {
        private static IngressState<string> CreateState()
        {
            return new IngressState<string>
            {
                _checkpointLock = new SemaphoreSlim(1, 1),
                _tokenSource = new CancellationTokenSource(),
                _linkCount = 0
            };
        }

        /// <summary>
        /// A sender that was waiting for the checkpoint lock while the stop cycle held it
        /// must re-check the gates after acquiring the lock: the stop barrier was injected
        /// while it waited, delivering the data now would land it after the final barrier
        /// and the next start would replay it, duplicating it at the sinks.
        /// </summary>
        [Fact]
        public async Task StopGateSetWhileWaitingForCheckpointLockParksTheSend()
        {
            var state = CreateState();
            var received = new BufferBlock<IStreamEvent>();
            var output = new IngressOutput<string>(state, received);

            // The sender starts outside the checkpoint lock, like a source between batches.
            await output.EnterCheckpointLock();
            output.ExitCheckpointLock();

            // The stop cycle holds the checkpoint lock while it injects the stop barrier.
            await state._checkpointLock!.WaitAsync();
            var sendTask = output.SendAsync("data");
            Assert.False(sendTask.IsCompleted);

            output.Stop();
            state._checkpointLock.Release();

            // The send must stay parked behind the stop gate instead of delivering.
            var finished = await Task.WhenAny(sendTask, Task.Delay(500));
            Assert.NotEqual(sendTask, finished);
            Assert.False(received.TryReceive(out _), "Data was delivered after the stop barrier");

            // Release the parked sender like a teardown would.
            state._tokenSource!.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sendTask);
        }

        /// <summary>
        /// Same as the send case but for watermarks, a watermark delivered after the stop
        /// barrier makes downstream consumers believe data completed that the barrier does
        /// not cover.
        /// </summary>
        [Fact]
        public async Task StopGateSetWhileWaitingForCheckpointLockParksTheWatermark()
        {
            var state = CreateState();
            var received = new BufferBlock<IStreamEvent>();
            var output = new IngressOutput<string>(state, received);
            state._vertexHandler = null;

            await output.EnterCheckpointLock();
            output.ExitCheckpointLock();

            await state._checkpointLock!.WaitAsync();
            var sendTask = output.SendAsync("data");
            Assert.False(sendTask.IsCompleted);

            output.Pause();
            state._checkpointLock.Release();

            var finished = await Task.WhenAny(sendTask, Task.Delay(500));
            Assert.NotEqual(sendTask, finished);
            Assert.False(received.TryReceive(out _), "Data was delivered after the pause gate was set");

            // A resume must release the parked sender and deliver the data.
            output.Resume();
            var finishedAfterResume = await Task.WhenAny(sendTask, Task.Delay(2000));
            Assert.Equal(sendTask, finishedAfterResume);
            await sendTask;
            Assert.True(received.TryReceive(out _));
        }
    }
}
