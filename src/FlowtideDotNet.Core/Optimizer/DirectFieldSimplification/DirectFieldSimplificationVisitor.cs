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
                for (int i = 0; i < projectRelation.Emit.Count; i++)
                {
                    if (projectRelation.Emit[i] < projectRelation.Input.OutputLength)
                    {
                        newEmitList.Add(projectRelation.Emit[i]);
                    }
                }
            }
            else
            {
               // Add only output from the input relation
               newEmitList.AddRange(Enumerable.Range(0, projectRelation.Input.OutputLength));
            }

            for (int i = 0; i < projectRelation.Expressions.Count; i++)
            {
                if (projectRelation.Expressions[i] is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment
                    && structReferenceSegment.Child == null)
                {
                    newEmitList.Add(structReferenceSegment.Field);
                    // Remove the expression since it is not needed
                    projectRelation.Expressions.RemoveAt(i);
                    i--;
                }
                else
                {
                    newEmitList.Add(projectRelation.Input.OutputLength + i);
                }
            }
            projectRelation.Emit = newEmitList;

            if (projectRelation.Expressions.Count == 0)
            {
                projectRelation.Input.Emit = projectRelation.Emit;
                return projectRelation.Input;   
            }
            return projectRelation;
        }
    }
}
