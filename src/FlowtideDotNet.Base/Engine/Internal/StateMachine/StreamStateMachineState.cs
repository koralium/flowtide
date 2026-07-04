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
    internal abstract class StreamStateMachineState
    {
        protected StreamContext? _context;

        public void SetContext(StreamContext context)
        {
            _context = context;
        }

        public abstract Task Initialize(StreamStateValue previousState);

        public abstract Task OnFailure();

        /// <summary>
        /// Called when an egress vertex has finished processing a locking event. The locking
        /// event is null when the signal comes from a checkpoint acknowledgement path that has
        /// no event instance, such acknowledgements are always checkpoint related.
        /// </summary>
        public abstract void EgressCheckpointDone(string name, ILockingEvent? lockingEvent);

        /// <summary>
        /// Called when an egress vertex has completed its dependencies for a locking event.
        /// The locking event is null when the signal comes from a checkpoint acknowledgement
        /// path that has no event instance, such acknowledgements are always checkpoint related.
        /// </summary>
        public abstract void EgressDependenciesDone(string name, ILockingEvent? lockingEvent);

        public abstract Task TriggerCheckpoint(bool isScheduled = false);

        public abstract Task CallTrigger(string operatorName, string triggerName, object? state);

        public abstract Task CallTrigger(string triggerName, object? state);

        public abstract Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null);

        public abstract Task StartAsync();

        public abstract Task DeleteAsync();

        public abstract Task StopAsync();

        protected Task TransitionTo(StreamStateValue newState)
        {
            Debug.Assert(_context != null, nameof(_context));
            return _context.TransitionTo(this, newState);
        }

        public virtual void Pause()
        {
        }

        public virtual void Resume()
        {
        }
    }
}
