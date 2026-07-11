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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.DistributedMode
{
    /// <summary>
    /// Bookkeeping for a union that combines the partition lanes of a distributed operator.
    /// Used by <see cref="LanePushdownVisitor"/> to push operators from above the union down
    /// into each lane, so they run inside the substreams before data is gathered.
    /// </summary>
    internal sealed class DistributedLaneUnion
    {
        public required SetRelation Union { get; init; }

        /// <summary>
        /// One entry per union input, in the same order.
        /// </summary>
        public required List<DistributedLaneInput> Inputs { get; init; }

        /// <summary>
        /// Columns in the union output carrying the partition key values, or null when unknown. Used
        /// to decide if stateful operators like aggregates can run in the lanes without a reshuffle.
        /// </summary>
        public List<int>? PartitionKeyColumns { get; set; }
    }

    /// <summary>
    /// Where one lane of a distributed union lives: either in the union's own substream (both
    /// properties null, the lane is the union input), or in another substream whose gather exchange
    /// sends its result back through the substream reference.
    /// </summary>
    internal sealed class DistributedLaneInput
    {
        public ExchangeRelation? GatherExchange { get; init; }

        public SubstreamExchangeReferenceRelation? Reference { get; init; }
    }
}
