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

namespace FlowtideDotNet.Core.Optimizer.EmitPushdown
{
    internal static class ReferenceEmitMerge
    {
        public static void MergeReferences(Plan plan, int index, List<ReferenceRelation> referenceRelations)
        {
            HashSet<int> mergedEmit = new HashSet<int>();

            foreach(var r in referenceRelations)
            {
                if (r.EmitSet)
                {
                    foreach (var e in r.Emit)
                    {
                        mergedEmit.Add(e);
                    }
                }
            }

            plan.Relations[index].Emit = mergedEmit.OrderBy(x => x).ToList();

            // Visit all relations and update their usage
            for(int i = 0; i < plan.Relations.Count; i++)
            {
                
            }
        }
    }
}
