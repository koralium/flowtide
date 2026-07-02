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
    /// Represents an event within the dataflow stream that requires operators and vertices to halt standard processing 
    /// and synchronize their state.
    /// </summary>
    /// <remarks>
    /// Locking events are used for critical operations such as taking checkpoints. When a sequence of locking events 
    /// is propagated, it ensures that data is consistently processed and stored up to that point across all branches 
    /// of the stream before proceeding.
    /// </remarks>
    public interface ILockingEvent : IStreamEvent
    {
    }
}
