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

using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Optimizer.DirectFieldSimplification
{
    internal class DirectFieldSimplificationVisitor : OptimizerBaseVisitor
    {
        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            
            var newEmitList = new List<int>();

            if (projectRelation.EmitSet)
            {
                newEmitList.AddRange(projectRelation.Emit);
            }
            else
            {
                // Add only output from the input relation
                newEmitList.AddRange(Enumerable.Range(0, projectRelation.Input.OutputLength));
                for (int i = 0; i < projectRelation.Expressions.Count; i++)
                {
                    newEmitList.Add(projectRelation.Input.OutputLength + i);
                }
            }

            for (int i = 0; i < newEmitList.Count; i++)
            {
                var emitIndex = newEmitList[i];

                if (emitIndex >= projectRelation.Input.OutputLength)
                {
                    var expressionIndex = emitIndex - projectRelation.Input.OutputLength;
                    var expression = projectRelation.Expressions[expressionIndex];
                    if (expression is DirectFieldReference directFieldReference &&
                        directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment
                        && structReferenceSegment.Child == null)
                    {
                        newEmitList[i] = structReferenceSegment.Field;
                        projectRelation.Expressions.RemoveAt(expressionIndex);

                        // Update the emit list to reflect the change
                        for (int j = 0; j < newEmitList.Count; j++)
                        {
                            if (newEmitList[j] > emitIndex)
                            {
                                newEmitList[j]--;
                            }
                        }
                    }
                }

            }
            projectRelation.Emit = newEmitList;
            return projectRelation;
        }

        private static List<int> CreateInputEmitList(Relation input, List<int> currentEmit)
        {
            // Create a new emit for the input
            var emit = new List<int>();
            Dictionary<int, int> inputEmitToInternal = new Dictionary<int, int>();
            if (input.EmitSet)
            {
                for (int i = 0; i < input.Emit.Count; i++)
                {
                    inputEmitToInternal.Add(i, input.Emit[i]);
                }
            }
            else
            {
                for (int i = 0; i < input.OutputLength; i++)
                {
                    inputEmitToInternal.Add(i, i);
                }
            }
            foreach (var field in currentEmit)
            {
                emit.Add(inputEmitToInternal[field]);
            }
            return emit;
        }
    }
}
