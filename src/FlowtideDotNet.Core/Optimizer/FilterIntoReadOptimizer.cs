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

namespace FlowtideDotNet.Core.Optimizer
{
    internal class FilterIntoReadOptimizer : OptimizerBaseVisitor
    {
        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            if (filterRelation.Input is ReadRelation readRelation)
            {
                readRelation.Filter = filterRelation.Condition;

                // Must handle emits here
                if (filterRelation.EmitSet)
                {
                    if (readRelation.EmitSet)
                    {
                        for(int i = 0; i < filterRelation.Emit.Count; i++)
                        {

                        }
                    }
                    else
                    {
                        // Read relation does not have emit set, move the emit down
                        readRelation.Emit = filterRelation.Emit;
                    }
                }

                return readRelation;
            }
            return base.VisitFilterRelation(filterRelation, state);
        }
    }
}
