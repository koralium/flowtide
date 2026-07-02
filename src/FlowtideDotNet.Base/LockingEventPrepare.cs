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
        /// <summary>
        /// Initializes a new instance of the <see cref="LockingEventPrepare"/> class with a new unique identifier.
        /// </summary>
        /// <param name="lockingEvent">The underlying locking event that this prepare message is associated with.</param>
        /// <param name="isInitEvent">Indicates if this is the initial checkpoint sequence.</param>
        public LockingEventPrepare(ILockingEvent lockingEvent, bool isInitEvent)
        {
            LockingEvent = lockingEvent;
            IsInitEvent = isInitEvent;
            OtherInputsNotInCheckpoint = false;
            Id = Guid.NewGuid();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LockingEventPrepare"/> class with an existing identifier and specific input state.
        /// </summary>
        /// <param name="lockingEvent">The underlying locking event that this prepare message is associated with.</param>
        /// <param name="isInitEvent">Indicates if this is the initial checkpoint sequence.</param>
        /// <param name="otherInputsNotInCheckpoint">Indicates if other inputs to a vertex have not yet entered the checkpoint state.</param>
        /// <param name="id">The unique identifier for this locking prepare message.</param>
        public LockingEventPrepare(ILockingEvent lockingEvent, bool isInitEvent, bool otherInputsNotInCheckpoint, Guid id)
        {
            LockingEvent = lockingEvent;
            IsInitEvent = isInitEvent;
            OtherInputsNotInCheckpoint = otherInputsNotInCheckpoint;
            Id = id;
        }

        /// <summary>
        /// Gets the actual locking event (such as a checkpoint event) that operators are preparing for.
        /// </summary>
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
