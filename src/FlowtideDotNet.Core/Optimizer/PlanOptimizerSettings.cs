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

using FlowtideDotNet.Core.Optimizer.DistributedMode;

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

        /// <summary>
        /// Finds identical subtrees in the plan and computes them only once,
        /// each usage references the shared result.
        /// </summary>
        public bool FindCommonSubPlans { get; set; } = true;

        public int Parallelization { get; set; } = 1;

        public bool TryAddWatermarkOutputMode { get; set; } = true;

        /// <summary>
        /// If set, the plan is split into substreams for distributed mode as the last
        /// optimization step. Merge joins, aggregates and window functions are partitioned
        /// with one partition per substream. Plans that already contain substream statements
        /// should not use this setting.
        /// When set, the <see cref="Parallelization"/> setting is not used, the substream count
        /// controls the partitioning instead.
        /// </summary>
        public DistributedPlanOptions? DistributedPlanOptions { get; set; }
    }
}
