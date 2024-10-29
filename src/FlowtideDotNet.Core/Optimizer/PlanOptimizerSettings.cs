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

namespace FlowtideDotNet.Core.Optimizer
{
    public class PlanOptimizerSettings
    {
        public bool NoMergeJoin { get; set; } = false;

        public bool GetTimestampToJoin { get; set; } = true;

        /// <summary>
        /// Automatically add a buffer block infront of get timestamp operations to reduce the number of outgoing events.
        /// </summary>
        public bool AddBufferBlockOnGetTimestamp { get; set; } = true;

        /// <summary>
        /// Tries to remove direct field references in projection.
        /// Disabled by default, since its not well tested yet.
        /// </summary>
        public bool SimplifyProjection { get; set; } = true;
    }
}
