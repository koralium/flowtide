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

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Message that is sent mostly by fixed point vertex to notify looped vertices that they should prepare for a checkpoint.
    /// This means that they should not generate any events from timed processes.
    /// This event can be sent multiple times in a loop, so the fixed point vertex can make sure all events have been processed.
    /// </summary>
    internal class LockingEventPrepare : IStreamEvent
    {
        public LockingEventPrepare(ILockingEvent lockingEvent, bool isInitEvent)
        {
            LockingEvent = lockingEvent;
            IsInitEvent = isInitEvent;
            OtherInputsNotInCheckpoint = false;
            Id = Guid.NewGuid();
        }

        public ILockingEvent LockingEvent { get; }
        
        /// <summary>
        /// Set only for init checkpoint, this is used so multiple input vertices can identify which inputs will send locking event prepares.
        /// </summary>
        public bool IsInitEvent { get; }

        /// <summary>
        /// Unique id of a locking event prepare message, this is used to allow operators to identify two different messages.
        /// </summary>
        public Guid Id { get; }

        /// <summary>
        /// Bool that is set if a multi target vertex has other inputs that are not in the checkpoint.
        /// This will stop the fixed point vertex from sending the locking event message until all other targets are in the checkpoint.
        /// This is required since those inputs might spawn events that will loop and will be excluded from the checkpoint.
        /// </summary>
        public bool OtherInputsNotInCheckpoint { get; set; }
    }
}
