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

using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class DeletingStreamState : StreamStateMachineState
    {
        // Bounds the retries so a delete that keeps failing surfaces the failure to the
        // callers instead of retrying forever, 20 attempts with the 500ms delay gives
        // transient storage faults ten seconds to clear.
        private const int MaxDeleteAttempts = 20;

        private readonly object _lock = new object();
        private Task? _deleteTask;
        private int _deleteAttempts;

        public override Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            return Task.CompletedTask;
        }

        public override Task CallTrigger(string operatorName, string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        public override Task CallTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        public override void EgressCheckpointDone(string name, ILockingEvent? lockingEvent)
        {
            // Ignore
        }

        public override void EgressDependenciesDone(string name, ILockingEvent? lockingEvent)
        {
            // Ignore
        }

        public override Task Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            // The delete supersedes a pause, the teardown must never park on the pause marker.
            lock (_lock)
            {
                _context.SetStatus(StreamStatus.Deleting);
                if (_deleteTask != null && !_deleteTask.IsCompleted)
                {
                    return Task.CompletedTask;
                }
                _deleteTask = Task.Factory.StartNew(async () =>
                {
                    await DeleteEntireStream();
                })
                .Unwrap()
                .ContinueWith(async t =>
                {
                    if (t.IsFaulted)
                    {
                        _deleteAttempts++;
                        if (_deleteAttempts >= MaxDeleteAttempts)
                        {
                            _context._logger.LogError(t.Exception, "Deleting the stream {stream} failed after {attempts} attempts, giving up. A new delete call starts fresh attempts.", _context.streamName, _deleteAttempts);
                            _deleteAttempts = 0;
                            _context.FailTeardownWaiters(t.Exception!);
                            return;
                        }
                        // A silently retried delete looks like a hung delete from the
                        // outside, every failed attempt is logged.
                        _context._logger.LogWarning(t.Exception, "Delete attempt {attempt} failed on stream {stream}, retrying.", _deleteAttempts, _context.streamName);
                        // Wait a while before trying to delete again
                        await Task.Delay(TimeSpan.FromMilliseconds(500));

                        await Initialize(StreamStateValue.Deleting);
                    }
                });
            }
            return Task.CompletedTask;
        }

        private async Task DeleteEntireStream()
        {
            Debug.Assert(_context != null, nameof(_context));

            _context.ForEachBlock((key, block) =>
            {
                block.Complete();
            });

            await Task.WhenAll(_context.GetCompletionTasks()).ContinueWith(t => { });

            await _context.ForEachBlockAsync(async (key, block) =>
            {
                await block.DisposeAsync();
            });

            // Call delete on all blocks
            await _context.ForEachBlockAsync(async (key, block) =>
            {
                await block.DeleteAsync();
            });

            _context._stateManager.Dispose();

            await TransitionTo(StreamStateValue.Deleted);
        }

        public override Task OnFailure()
        {
            return Task.CompletedTask;
        }

        public override Task StartAsync()
        {
            return Task.CompletedTask;
        }

        public override Task TriggerCheckpoint(bool isScheduled = false)
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            // Restarts the attempts when an earlier delete gave up, while a delete runs
            // this is a no-op through the running task guard in Initialize.
            return Initialize(StreamStateValue.Deleting);
        }

        public override Task StopAsync()
        {
            // A delete implies the stop. The stop task the caller awaits was created before
            // this state was consulted, the deleted state completes it together with the
            // delete task when the delete has finished.
            return Task.CompletedTask;
        }
    }
}
