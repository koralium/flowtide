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

namespace FlowtideDotNet.Core.Optimizer
{
    /// <summary>
    /// A visitor that can be used to more easily find specific expressions
    /// </summary>
    /// <typeparam name="TOutput"></typeparam>
    internal abstract class BaseRelationExpressionVisitor<TOutput> : OptimizerBaseVisitor
    {

        public abstract ExpressionVisitor<TOutput, object> Visitor { get; }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            if (aggregateRelation.Measures != null)
            {
                foreach (var measure in aggregateRelation.Measures)
                {
                    if (measure.Filter != null)
                    {
                        Visitor.Visit(measure.Filter, state);
                    }
                    if (measure.Measure != null)
                    {
                        foreach (var arg in measure.Measure.Arguments)
                        {
                            Visitor.Visit(arg, state);
                        }
                    }
                }
            }
            return base.VisitAggregateRelation(aggregateRelation, state);
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object state)
        {
            foreach (var windowFunction in consistentPartitionWindowRelation.WindowFunctions)
            {
                foreach (var arg in windowFunction.Arguments)
                {
                    Visitor.Visit(arg, state);
                }
            }

            foreach (var partitionBy in consistentPartitionWindowRelation.PartitionBy)
            {
                Visitor.Visit(partitionBy, state);
            }

            foreach (var orderBy in consistentPartitionWindowRelation.OrderBy)
            {
                Visitor.Visit(orderBy.Expression, state);
            }

            return base.VisitConsistentPartitionWindowRelation(consistentPartitionWindowRelation, state);
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            Visitor.Visit(filterRelation.Condition, state);
            return base.VisitFilterRelation(filterRelation, state);
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            if (joinRelation.Expression != null)
            {
                Visitor.Visit(joinRelation.Expression, state);
            }

            if (joinRelation.PostJoinFilter != null)
            {
                Visitor.Visit(joinRelation.PostJoinFilter, state);
            }

            return base.VisitJoinRelation(joinRelation, state);
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            foreach (var leftKey in mergeJoinRelation.LeftKeys)
            {
                Visitor.Visit(leftKey, state);
            }
            foreach (var rightKey in mergeJoinRelation.RightKeys)
            {
                Visitor.Visit(rightKey, state);
            }
            if (mergeJoinRelation.PostJoinFilter != null)
            {
                Visitor.Visit(mergeJoinRelation.PostJoinFilter, state);
            }
            return base.VisitMergeJoinRelation(mergeJoinRelation, state);
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            if (normalizationRelation.Filter != null)
            {
                Visitor.Visit(normalizationRelation.Filter, state);
            }
            return base.VisitNormalizationRelation(normalizationRelation, state);
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            foreach (var projection in projectRelation.Expressions)
            {
                Visitor.Visit(projection, state);
            }
            return base.VisitProjectRelation(projectRelation, state);
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (readRelation.Filter != null)
            {
                Visitor.Visit(readRelation.Filter, state);
            }
            return base.VisitReadRelation(readRelation, state);
        }

        public override Relation VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, object state)
        {
            if (tableFunctionRelation.JoinCondition != null)
            {
                Visitor.Visit(tableFunctionRelation.JoinCondition, state);
            }

            if (tableFunctionRelation.TableFunction != null)
            {
                foreach (var arg in tableFunctionRelation.TableFunction.Arguments)
                {
                    Visitor.Visit(arg, state);
                }
            }
            return base.VisitTableFunctionRelation(tableFunctionRelation, state);
        }

        public override Relation VisitSortRelation(SortRelation sortRelation, object state)
        {
            foreach (var sort in sortRelation.Sorts)
            {
                Visitor.Visit(sort.Expression, state);
            }
            return base.VisitSortRelation(sortRelation, state);
        }

        public override Relation VisitTopNRelation(TopNRelation topNRelation, object state)
        {
            foreach (var sort in topNRelation.Sorts)
            {
                Visitor.Visit(sort.Expression, state);
            }
            return base.VisitTopNRelation(topNRelation, state);
        }

        public override Relation VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, object state)
        {
            foreach (var expr in virtualTableReadRelation.Values.Expressions)
            {
                foreach (var field in expr.Fields)
                {
                    Visitor.Visit(field, state);
                }
            }
            return base.VisitVirtualTableReadRelation(virtualTableReadRelation, state);
        }
    }
}
