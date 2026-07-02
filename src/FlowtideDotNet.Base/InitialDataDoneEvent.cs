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
    /// Represents an event that signals the completion of the initial data load for a partition or stream.
    /// </summary>
    /// <remarks>
    /// This event is primarily used by ingress vertices to notify downstream operators 
    /// that the historical or initial full dataset has been sent, and that subsequent data 
    /// will represent ongoing changes or deltas. This helps operators correctly align 
    /// watermarks and checkpoint states immediately following startup.
    /// </remarks>
    internal class InitialDataDoneEvent : IStreamEvent
    {
    }
}
