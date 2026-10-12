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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.GetTimestamp;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    // Overrides return their input, the plan stays untouched.
    internal sealed class LineageTableCollector : OptimizerBaseVisitor
    {
        public List<ReadRelation> Reads { get; } = new List<ReadRelation>();

        public List<WriteRelation> Writes { get; } = new List<WriteRelation>();

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (readRelation.NamedTable.DotSeperated != GetTimestampVisitor.GetTimestampTableName)
            {
                Reads.Add(readRelation);
            }
            return readRelation;
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            Writes.Add(writeRelation);
            return base.VisitWriteRelation(writeRelation, state);
        }

        public override Relation VisitPlanRelation(PlanRelation planRelation, object state)
        {
            Visit(planRelation.Root, state);
            return planRelation;
        }
    }
}
