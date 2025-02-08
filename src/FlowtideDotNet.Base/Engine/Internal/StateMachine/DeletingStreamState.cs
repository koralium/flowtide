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

using System.Diagnostics;

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class DeletingStreamState : StreamStateMachineState
    {
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

        public override void EgressCheckpointDone(string name)
        {
            // Ignore
        }

        public override void Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            _context.CheckForPause();
            lock (_lock)
            {
                _context.SetStatus(StreamStatus.Deleting);
                if (_deleteTask != null && !_deleteTask.IsCompleted)
                {
                    return;
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
                        // Wait a while before trying to delete again
                        await Task.Delay(TimeSpan.FromMilliseconds(500));

                        Initialize(StreamStateValue.Deleting);
                    }
                });
            }
            
        }

        private async Task DeleteEntireStream()
        {
            Debug.Assert(_context != null, nameof(_context));
            
            _context.ForEachBlock((key,block) =>
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
            return Task.CompletedTask;
        }

        public override Task StopAsync()
        {
            throw new NotSupportedException("Stream is being deleted");
        }
    }
}
