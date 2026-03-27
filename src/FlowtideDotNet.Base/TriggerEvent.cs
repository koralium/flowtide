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
    /// Represents an event that should invoke a registered trigger within the stream.
    /// </summary>
    /// <remarks>
    /// A <see cref="TriggerEvent"/> is typically dispatched to a vertex when a set interval elapses 
    /// or when a trigger is invoked manually from outside the stream. This allows vertices to perform 
    /// periodic actions, background tasks, or custom state flushes.
    /// </remarks>
    public class TriggerEvent : IStreamEvent
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TriggerEvent"/> class.
        /// </summary>
        /// <param name="name">The name of the registered trigger to be fired.</param>
        /// <param name="state">Optional state associated with the trigger invocation.</param>
        public TriggerEvent(string name, object? state)
        {
            Name = name;
            State = state;
        }

        /// <summary>
        /// Gets the name of the trigger that should be invoked.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets any optional state or payload associated with the trigger execution.
        /// </summary>
        public object? State { get; }
    }
}
