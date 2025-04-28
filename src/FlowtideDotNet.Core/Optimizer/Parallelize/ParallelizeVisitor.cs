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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Optimizer.MergeJoinParallelize
{
    /// <summary>
    /// This optmiziation adds exchange relations infront of merge joins which helps to parallelize the merge join operation.
    /// </summary>
    internal class ParallelizeVisitor : OptimizerBaseVisitor
    {
        private readonly int parallelCount;
        private Plan? _plan;

        public override Plan VisitPlan(Plan plan)
        {
            _plan = plan;
            return base.VisitPlan(plan);
        }

        public ParallelizeVisitor(int parallelCount)
        {
            this.parallelCount = parallelCount;
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object state)
        {
            Debug.Assert(_plan != null);

            if (parallelCount < 2 || (consistentPartitionWindowRelation.PartitionBy.Count < 1))
            {
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            if (consistentPartitionWindowRelation.WindowFunctions.Any(x => x.ExtensionUri == FunctionsAggregateGeneric.Uri && x.ExtensionName == FunctionsAggregateGeneric.SurrogateKeyInt64))
            {
                // Surrogate key is not supported in parallelization
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            List<FieldReference> fields = new List<FieldReference>();

            foreach (var field in consistentPartitionWindowRelation.PartitionBy)
            {
                if (field is DirectFieldReference directFieldReference)
                {
                    fields.Add(directFieldReference);
                }
            }

            if (fields.Count == 0)
            {
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            List<ExchangeTarget> exchangeTargets = new List<ExchangeTarget>();
            for (int i = 0; i < parallelCount; i++)
            {
                exchangeTargets.Add(new StandardOutputExchangeTarget()
                {
                    PartitionIds = new List<int>() { i }
                });
            }

            var exchange = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = fields
                },
                Input = consistentPartitionWindowRelation.Input,
                Targets = exchangeTargets,
                PartitionCount = parallelCount
            };

            var exchangeIndex = _plan.Relations.Count;
            _plan.Relations.Add(exchange);

            ConsistentPartitionWindowRelation[] parallelWindows = new ConsistentPartitionWindowRelation[parallelCount];

            for (int i = 0; i < parallelCount; i++)
            {
                parallelWindows[i] = new ConsistentPartitionWindowRelation()
                {
                    Input = new StandardOutputExchangeReferenceRelation()
                    {
                        TargetId = i,
                        ReferenceOutputLength = exchange.OutputLength,
                        RelationId = exchangeIndex
                    },
                    PartitionBy = consistentPartitionWindowRelation.PartitionBy,
                    OrderBy = consistentPartitionWindowRelation.OrderBy,
                    WindowFunctions = consistentPartitionWindowRelation.WindowFunctions,
                    Emit = consistentPartitionWindowRelation.Emit
                };
            }

            var setRelation = new SetRelation()
            {
                Inputs = new List<Relation>(parallelWindows),
                Operation = SetOperation.UnionAll
            };

            return setRelation;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            Debug.Assert(_plan != null);
            if (parallelCount < 2 || (aggregateRelation.Groupings == null || aggregateRelation.Groupings.Count < 1))
            {
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            if (aggregateRelation.Measures != null &&
                aggregateRelation.Measures.Any(x => x.Measure.ExtensionUri == FunctionsAggregateGeneric.Uri &&
                    x.Measure.ExtensionName == FunctionsAggregateGeneric.SurrogateKeyInt64))
            {
                // Surrogate key is not supported in parallelization
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            List<FieldReference> fields = new List<FieldReference>();

            foreach(var grouping in aggregateRelation.Groupings)
            {
                foreach(var field in grouping.GroupingExpressions)
                {
                    if (field is DirectFieldReference directFieldReference)
                    {
                        fields.Add(directFieldReference);
                    }
                }
            }

            if (fields.Count == 0)
            {
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            List<ExchangeTarget> exchangeTargets = new List<ExchangeTarget>();

            for (int i = 0; i < parallelCount; i++)
            {
                exchangeTargets.Add(new StandardOutputExchangeTarget()
                {
                    PartitionIds = new List<int>() { i }
                });
            }

            var exchange = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = fields
                },
                Input = aggregateRelation.Input,
                Targets = exchangeTargets,
                PartitionCount = parallelCount
            };

            var exchangeIndex = _plan.Relations.Count;
            _plan.Relations.Add(exchange);

            AggregateRelation[] parallelAggregates = new AggregateRelation[parallelCount];

            for (int i = 0; i < parallelCount; i++)
            {
                parallelAggregates[i] = new AggregateRelation()
                {
                    Input = new StandardOutputExchangeReferenceRelation()
                    {
                        TargetId = i,
                        ReferenceOutputLength = exchange.OutputLength,
                        RelationId = exchangeIndex
                    },
                    Groupings = aggregateRelation.Groupings,
                    Measures = aggregateRelation.Measures,
                    Emit = aggregateRelation.Emit
                };
            }

            var setRelation = new SetRelation()
            {
                Inputs = new List<Relation>(parallelAggregates),
                Operation = SetOperation.UnionAll
            };

            return setRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            Debug.Assert(_plan != null);
            if (parallelCount < 2)
            {
                return mergeJoinRelation;
            }

            List<ExchangeTarget> exchangeTargets = new List<ExchangeTarget>();

            for (int i = 0; i < parallelCount; i++)
            {
                exchangeTargets.Add(new StandardOutputExchangeTarget()
                {
                    PartitionIds = new List<int>() { i }
                });
            }

            var leftExchange = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = mergeJoinRelation.LeftKeys
                },
                Input = mergeJoinRelation.Left,
                Targets = exchangeTargets,
                PartitionCount = parallelCount
            };
            var rightExchange = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = mergeJoinRelation.RightKeys.Select(x =>
                    {
                        if (x is DirectFieldReference directField)
                        {
                            if (directField.ReferenceSegment is StructReferenceSegment structRef)
                            {
                                return new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = structRef.Field - mergeJoinRelation.Left.OutputLength,
                                        Child = structRef.Child
                                    }
                                };
                            }
                        }
                        return x;
                    }).ToList()
                },
                Input = mergeJoinRelation.Right,
                Targets = exchangeTargets,
                PartitionCount = parallelCount
            };

            var leftIndex = _plan.Relations.Count;
            _plan.Relations.Add(leftExchange);
            var rightIndex = _plan.Relations.Count;
            _plan.Relations.Add(rightExchange);

            MergeJoinRelation[] parallelMergeJoins = new MergeJoinRelation[parallelCount];

            for (int i = 0; i < parallelCount; i++)
            {
                parallelMergeJoins[i] = new MergeJoinRelation()
                {
                    Left = new StandardOutputExchangeReferenceRelation()
                    {
                        TargetId = i,
                        ReferenceOutputLength = leftExchange.OutputLength,
                        RelationId = leftIndex
                    },
                    Right = new StandardOutputExchangeReferenceRelation()
                    {
                        TargetId = i,
                        RelationId = rightIndex,
                        ReferenceOutputLength = rightExchange.OutputLength
                    },
                    LeftKeys = mergeJoinRelation.LeftKeys,
                    RightKeys = mergeJoinRelation.RightKeys,
                    Type = mergeJoinRelation.Type,
                    PostJoinFilter = mergeJoinRelation.PostJoinFilter,
                    Emit = mergeJoinRelation.Emit
                };
            }

            var setRelation = new SetRelation()
            {
                Inputs = new List<Relation>(parallelMergeJoins),
                Operation = SetOperation.UnionAll
            };

            return setRelation;
        }
    }
}
