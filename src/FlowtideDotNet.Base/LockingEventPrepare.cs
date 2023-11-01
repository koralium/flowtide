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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Message that is sent mostly by fixed point vertex to notify looped vertices that they should prepare for a checkpoint.
    /// This means that they should not generate any events from timed processes.
    /// This event can be sent multiple times in a loop, so the fixed point vertex can make sure all events have been processed.
    /// </summary>
    internal class LockingEventPrepare : IStreamEvent
    {
        public LockingEventPrepare(ILockingEvent lockingEvent)
        {
            LockingEvent = lockingEvent;
            OtherInputsNotInCheckpoint = false;
        }

        public ILockingEvent LockingEvent { get; }

        /// <summary>
        /// Bool that is set if a multi target vertex has other inputs that are not in the checkpoint.
        /// This will stop the fixed point vertex from sending the locking event message until all other targets are in the checkpoint.
        /// This is required since those inputs might spawn events that will loop and will be excluded from the checkpoint.
        /// </summary>
        public bool OtherInputsNotInCheckpoint { get; set; }
    }
}
