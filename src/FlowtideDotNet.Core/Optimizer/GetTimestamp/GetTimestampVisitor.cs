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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Optimizer.GetTimestamp
{
    /// <summary>
    /// This optimizer expects to be run before merge join
    /// </summary>
    internal class GetTimestampVisitor : OptimizerBaseVisitor
    {
        private readonly Plan plan;
        private readonly PlanOptimizerSettings planOptimizerSettings;
        private int? referenceIndex;

        public GetTimestampVisitor(Plan plan, PlanOptimizerSettings planOptimizerSettings)
        {
            this.plan = plan;
            this.planOptimizerSettings = planOptimizerSettings;
        }

        private int GetReferenceIndex()
        {
            if (referenceIndex != null)
            {
                return referenceIndex.Value;
            }
            else
            {
                // Add a read relation to the plan to get the current timestamp
                referenceIndex = plan.Relations.Count;
                plan.Relations.Add(new ReadRelation()
                {
                    BaseSchema = new NamedStruct
                    {
                        Names = new List<string>() { "timestamp" },
                        Struct = new Struct()
                        {
                            Types = new List<SubstraitBaseType>()
                            {
                                new DateType()
                            }
                        }
                    },
                    NamedTable = new NamedTable()
                    {
                        Names = new List<string>() { "__gettimestamp" },
                    }
                });
                return referenceIndex.Value;
            }
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            // TODO: It might be possible here to move the filter that contains the date to the join instead.
            // This would reduce the amount of data that is sent out.
            // For now this implementation just injects the timestamp

            filterRelation.Input = Visit(filterRelation.Input, state);

            var replacer = new GetTimestampReplacer(filterRelation.Input.OutputLength);

            filterRelation.Condition = replacer.Visit(filterRelation.Condition, state)!;

            if (!replacer.ContainsGetTimestamp)
            {
                return filterRelation;
            }

            var emit = filterRelation.Emit;

            if (emit == null)
            {
                emit = new List<int>();
                for (int i = 0; i < filterRelation.OutputLength; i++)
                {
                    emit.Add(i);
                }
            }

            var tmpInput = filterRelation.Input;

            var join = new JoinRelation()
            {
                Emit = emit,
                Left = tmpInput,
                Right = new ReferenceRelation()
                {
                    ReferenceOutputLength = 1,
                    RelationId = GetReferenceIndex()
                },
                Expression = filterRelation.Condition,
                Type = JoinType.Inner
            };

            if (planOptimizerSettings.AddBufferBlockOnGetTimestamp)
            {
                // Add a buffer block to make sure that the timestamp is not sent out before the join is done
                var bufferBlock = new BufferRelation()
                {
                    Input = join
                };
                return bufferBlock;
            }

            return join;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, state);

            var replacer = new GetTimestampReplacer(aggregateRelation.Input.OutputLength);

            if (aggregateRelation.Groupings != null)
            {
                for (int i = 0; i < aggregateRelation.Groupings.Count; i++)
                {
                    for (int k = 0; k < aggregateRelation.Groupings[i].GroupingExpressions.Count; k++)
                    {
                        aggregateRelation.Groupings[i].GroupingExpressions[k] = replacer.Visit(aggregateRelation.Groupings[i].GroupingExpressions[k], state)!;
                    }
                }
            }

            if (aggregateRelation.Measures != null)
            {
                for (int i = 0; i < aggregateRelation.Measures.Count; i++)
                {
                    aggregateRelation.Measures[i].Filter = replacer.Visit(aggregateRelation.Measures[i].Filter!, state)!;
                    for (int k = 0; k < aggregateRelation.Measures[i].Measure.Arguments.Count; k++)
                    {
                        aggregateRelation.Measures[i].Measure.Arguments[k] = replacer.Visit(aggregateRelation.Measures[i].Measure.Arguments[k], state)!;
                    }
                }
            }

            if (!replacer.ContainsGetTimestamp)
            {
                return aggregateRelation;
            }

            var tmpInput = aggregateRelation.Input;

            if (!aggregateRelation.EmitSet)
            {
                // Add all fields but before adding the join, so the get timestamp value is not included in the output
                List<int> emitList = new List<int>();
                for (int i = 0; i < aggregateRelation.OutputLength; i++)
                {
                    emitList.Add(i);
                }
                aggregateRelation.Emit = emitList;
            }

            var crossJoin = new JoinRelation()
            {
                Left = tmpInput,
                Right = new ReferenceRelation()
                {
                    ReferenceOutputLength = 1,
                    RelationId = GetReferenceIndex()
                },
                Expression = new BoolLiteral() { Value = true },
                Type = JoinType.Inner
            };

            aggregateRelation.Input = crossJoin;

            if (planOptimizerSettings.AddBufferBlockOnGetTimestamp)
            {
                // Add a buffer block to make sure that the timestamp is not sent out before the join is done
                var bufferBlock = new BufferRelation()
                {
                    Input = aggregateRelation
                };
                return bufferBlock;
            }
            return aggregateRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);

            var replacer = new GetTimestampReplacer(joinRelation.Left.OutputLength + joinRelation.Right.OutputLength);
            replacer.Visit(joinRelation.Expression!, state);

            if (replacer.ContainsGetTimestamp)
            {
                // Skip joins for now, requires more advanced logic on how to inject the timestamp
                throw new InvalidOperationException("gettimestamp is not supported in join conditions yet.");
            }

            return base.VisitJoinRelation(joinRelation, state);
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            // Check if the project relation contains get_timestamp
            // If it does, we replace it with a column reference.
            // Add a join infront of the project relation against a reference

            projectRelation.Input = Visit(projectRelation.Input, state);

            var replacer = new GetTimestampReplacer(projectRelation.Input.OutputLength);
            
            for (int i = 0; i < projectRelation.Expressions.Count; i++)
            {
                projectRelation.Expressions[i] = replacer.Visit(projectRelation.Expressions[i], state)!;
            }

            if (!replacer.ContainsGetTimestamp)
            {
                return projectRelation;
            }

            var tmpInput = projectRelation.Input;
            // If emit is already set, we dont need to do anything more, only more fields are appended to the right
            if (!projectRelation.EmitSet)
            {
                // Add all fields but before adding the join, to make sure that 
                List<int> emitList = new List<int>();
                for (int i = 0; i < projectRelation.OutputLength; i++)
                {
                    emitList.Add(i);
                }
                projectRelation.Emit = emitList;
            }
            else
            {
                // If emit is set, we need to increase the emit index for all fields that are after the get_timestamp
                var emit = projectRelation.Emit!;
                for (int i = 0; i < emit.Count; i++)
                {
                    if (emit[i] >= tmpInput.OutputLength)
                    {
                        emit[i] += 1;
                    }
                }
            }

            var crossJoin = new JoinRelation()
            {
                Left = tmpInput,
                Right = new ReferenceRelation()
                {
                    ReferenceOutputLength = 1,
                    RelationId = GetReferenceIndex()
                },
                Expression = new BoolLiteral() { Value = true },
                Type = JoinType.Inner
            };

            projectRelation.Input = crossJoin;

            if (planOptimizerSettings.AddBufferBlockOnGetTimestamp)
            {
                // Add a buffer block to make sure that the timestamp is not sent out before the join is done
                var bufferBlock = new BufferRelation()
                {
                    Input = projectRelation
                };
                return bufferBlock;
            }

            return projectRelation;
        }
    }
}
