﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Base.Engine.Internal.StateMachine
{
    internal class DeletedStreamState : StreamStateMachineState
    {
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

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override void EgressCheckpointDone(string name)
        {
        }

        public override void Initialize(StreamStateValue previousState)
        {
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
    }
}
