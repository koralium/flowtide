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

namespace FlowtideDotNet.Core.Optimizer.DistributedMode
{
    /// <summary>
    /// Options for <see cref="DistributedPlanModifier"/> that controls how a plan
    /// is split into substreams.
    /// </summary>
    public class DistributedPlanOptions
    {
        /// <summary>
        /// The number of substreams to split the plan into.
        /// Sink roots are assigned to substreams round robin, so the effective number of
        /// substreams is at most the number of sink roots in the plan.
        /// </summary>
        public required int SubstreamCount { get; init; }

        /// <summary>
        /// Optional generator for substream names, gets the substream index as input.
        /// Defaults to "substream_{index}".
        /// </summary>
        public Func<int, string>? SubstreamNameGenerator { get; init; }
    }
}
