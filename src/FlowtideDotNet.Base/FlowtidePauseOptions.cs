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
    /// Represents the options to dynamically pause and resume a Flowtide dataflow stream.
    /// </summary>
    /// <remarks>
    /// This class is typically used in conjunction with <c>IOptionsMonitor&lt;FlowtidePauseOptions&gt;</c>.
    /// By subscribing to changes via the options monitor, the engine can halt or resume event propagation 
    /// at runtime without needing to tear down or restart the stream.
    /// </remarks>
    public class FlowtidePauseOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether the stream should be paused.
        /// </summary>
        /// <remarks>
        /// When set to <c>true</c>, the stream will temporarily stop processing new events.
        /// Changing this value to <c>false</c> will cause the stream to resume normal processing operations.
        /// </remarks>
        public bool IsPaused { get; set; }
    }
}
