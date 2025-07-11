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
    internal class NotStartedStreamState : StreamStateMachineState
    {
        public override Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null)
        {
            throw new NotSupportedException("Stream not started");
        }

        public override Task CallTrigger(string operatorName, string triggerName, object? state)
        {
            throw new NotSupportedException("Stream not started");
        }

        public override Task CallTrigger(string triggerName, object? state)
        {
            throw new NotSupportedException("Stream not started");
        }

        public override Task DeleteAsync()
        {
            Debug.Assert(_context != null, nameof(_context));

            // Blocks must be created before delete can be called
            _context.ForEachBlock((key, block) =>
            {
                block.Setup(_context.streamName, key);
                block.CreateBlock();
            });
            return TransitionTo(StreamStateValue.Deleting);
        }

        public override void EgressCheckpointDone(string name)
        {

        }

        public override void Initialize(StreamStateValue previousState)
        {
            Debug.Assert(_context != null, nameof(_context));
            _context.SetStatus(StreamStatus.Stopped);
        }

        public override Task OnFailure()
        {
            return Task.CompletedTask;
        }

        public override Task StartAsync()
        {
            Debug.Assert(_context != null);
            _context._wantedState = StreamStateValue.Running;
            return TransitionTo(StreamStateValue.Starting);
        }

        public override Task StopAsync()
        {
            return Task.CompletedTask;
        }

        public override Task TriggerCheckpoint(bool isScheduled = false)
        {
            return Task.CompletedTask;
        }
    }
}
