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

using FlowtideDotNet.Substrait;

namespace FlowtideDotNet.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class StartStreamRequest
    {
        /// <summary>
        /// Starts the stream from SQL text. The stream grain compiles and optimizes the SQL
        /// centrally using the connectors registered in the silo, so a thin client without
        /// connector configuration can start streams this way.
        /// </summary>
        public StartStreamRequest(string sqlText, int? substreamCount = null)
        {
            SqlText = sqlText;
            SubstreamCount = substreamCount;
            OptimizePlan = true;
        }

        private StartStreamRequest(string? sqlText, string? planJson, int? substreamCount, bool optimizePlan)
        {
            SqlText = sqlText;
            PlanJson = planJson;
            SubstreamCount = substreamCount;
            OptimizePlan = optimizePlan;
        }

        /// <summary>
        /// Starts the stream from a user created plan instead of SQL text. The plan is
        /// serialized and prepared centrally by the stream grain: with
        /// <paramref name="optimizePlan"/> set it runs through the plan optimizer, splitting
        /// it into <paramref name="substreamCount"/> substreams when the count is given and
        /// the plan contains no substream roots. Pass <paramref name="optimizePlan"/> false
        /// for a plan that is already optimized (or already distributed), it then runs
        /// exactly as given.
        /// </summary>
        public static StartStreamRequest FromPlan(Plan plan, int? substreamCount = null, bool optimizePlan = true)
        {
            return new StartStreamRequest(null, SubstraitSerializer.SerializeToJson(plan), substreamCount, optimizePlan);
        }

        [Id(0)]
        public string? SqlText { get; }

        /// <summary>
        /// If set, a plan without substream statements is split automatically into this many
        /// substreams using the distributed plan modifier.
        /// </summary>
        [Id(1)]
        public int? SubstreamCount { get; }

        /// <summary>
        /// The user created plan as serialized Substrait JSON, exactly one of this and
        /// <see cref="SqlText"/> is set.
        /// </summary>
        [Id(2)]
        public string? PlanJson { get; }

        /// <summary>
        /// Whether the stream grain runs the plan optimizer over the plan. Always true for
        /// SQL requests; false lets a user created plan run exactly as given.
        /// </summary>
        [Id(3)]
        public bool OptimizePlan { get; }
    }
}
