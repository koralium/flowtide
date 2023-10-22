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

        public abstract void Initialize();

        public abstract Task OnFailure();

        public abstract void EgressCheckpointDone(string name);

        public abstract Task TriggerCheckpoint(bool isScheduled = false);

        public abstract Task CallTrigger(string operatorName, string triggerName, object? state);

        public abstract Task CallTrigger(string triggerName, object? state);

        public abstract Task AddTrigger(string operatorName, string triggerName, TimeSpan? schedule = null);

        public abstract Task StartAsync();

        public abstract Task DeleteAsync();

        protected Task TransitionTo(StreamStateValue newState)
        {
            Debug.Assert(_context != null, nameof(_context));
            return _context.TransitionTo(this, newState);
        }
    }
}
