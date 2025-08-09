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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.WatermarkOutput
{
    /// <summary>
    /// Visitor that cleans up the temporary optimization properties added to reference relations
    /// </summary>
    internal class WatermarkOutputCleanupVisitor : OptimizerBaseVisitor
    {
        public void Run(Plan plan)
        {
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                var relation = plan.Relations[i];
                this.Visit(relation, null!);
            }
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
        {
            // Remove the temporary optimization property
            referenceRelation.Hint.Optimizations.Properties.Remove(WatermarkOutputUtils.ReferenceRelationPropertyName);
            return base.VisitReferenceRelation(referenceRelation, state);
        }
    }
}
