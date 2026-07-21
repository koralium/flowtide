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

using FlowtideDotNet.Base.Exceptions;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class DeletingStreamState : StreamStateMachineState
    {
        // Bounds the retries so a delete that keeps failing surfaces the failure to the
        // callers instead of retrying forever, 20 attempts with the 500ms delay gives
        // transient storage faults ten seconds to clear. Internal so tests can shorten them.
        internal static int MaxDeleteAttempts = 20;
        internal static TimeSpan DeleteRetryDelay = TimeSpan.FromMilliseconds(500);

        private readonly object _lock = new object();
        private Task? _deleteTask;

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
                // A single task owns every attempt. It is only replaced once it has
                // completed, so a concurrent delete or a retry can never start a second
                // teardown running against the same blocks and state manager.
                if (_deleteTask != null && !_deleteTask.IsCompleted)
                {
                    return Task.CompletedTask;
                }
                _deleteTask = Task.Run(RunDeleteWithRetries);
            }
            return Task.CompletedTask;
        }

        private async Task RunDeleteWithRetries()
        {
            Debug.Assert(_context != null, nameof(_context));
            Exception? lastError = null;
            for (int attempt = 1; attempt <= MaxDeleteAttempts; attempt++)
            {
                try
                {
                    if (attempt > 1)
                    {
                        // Wait, then re-create the blocks the previous attempt faulted and
                        // disposed, delete needs live blocks. Dispose is not idempotent in
                        // every operator, so retrying against disposed blocks would turn a
                        // transient storage fault into a permanent failure. This is the same
                        // precondition NotStartedStreamState.DeleteAsync establishes.
                        await Task.Delay(DeleteRetryDelay);
                        _context.ForEachBlock((key, block) =>
                        {
                            block.Setup(_context.streamName, key);
                            block.CreateBlock();
                        });
                        lock (_context._blockClaimLock)
                        {
                            _context._blockGeneration++;
                            _context._blocksCreated = 1;
                        }
                    }
                    await DeleteEntireStream();
                    return;
                }
                catch (Exception e)
                {
                    lastError = e;
                    // A silently retried delete looks like a hung delete from the outside,
                    // every failed attempt is logged.
                    _context._logger.LogWarning(e, "Delete attempt {attempt} failed on stream {stream}.", attempt, _context.streamName);
                }
            }
            // Every attempt failed, surface it to the callers instead of retrying forever.
            // The stream stays in the deleting state, a new delete call restarts the
            // attempts and a stop is honored by StopAsync since the blocks are torn down.
            _context._logger.LogError(lastError, "Deleting the stream {stream} failed after {attempts} attempts, giving up. A new delete call starts fresh attempts.", _context.streamName, MaxDeleteAttempts);
            _context.FailTeardownWaiters(lastError!);
        }

        private async Task DeleteEntireStream()
        {
            Debug.Assert(_context != null, nameof(_context));

            // The blocks are faulted, not completed: a delete can land on a pipeline with
            // in flight data that has nowhere to go, and graceful completion then waits
            // forever for a drain that cannot happen. Discarding is always correct here,
            // the delete destroys all state anyway.
            _context.ForEachBlock((key, block) =>
            {
                block.Fault(new BlockStopException("Faulting block due to stream deletion."));
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
            Debug.Assert(_context != null, nameof(_context));
            // Starting a stream that is being deleted is a caller error, silently reporting
            // success would let a restart loop believe the stream came back while it stays
            // deleted.
            throw new NotSupportedException($"Stream '{_context.streamName}' is being deleted and cannot be started.");
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
            Debug.Assert(_context != null, nameof(_context));
            Task? deleteTask;
            lock (_lock)
            {
                deleteTask = _deleteTask;
            }
            if (deleteTask != null && !deleteTask.IsCompleted)
            {
                // A delete is running, a delete implies the stop. The deleted state completes
                // the stop task together with the delete task when the delete has finished.
                return Task.CompletedTask;
            }
            // No delete is running, for example it gave up after exhausting its attempts.
            // The blocks are already faulted and disposed so the stop has nothing left to
            // do, its task must be completed or the caller waits forever. Shares the
            // checkpoint lock with the deleted state so only one of them completes it.
            lock (_context._checkpointLock)
            {
                if (_context._stopTask != null)
                {
                    _context._stopTask.SetResult();
                    _context._stopTask = null;
                }
            }
            return Task.CompletedTask;
        }
    }
}
