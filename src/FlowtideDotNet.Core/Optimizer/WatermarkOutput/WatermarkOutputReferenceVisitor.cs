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
    /// Visitor that identifies which relations can run every batch based on their references.
    /// </summary>
    internal class WatermarkOutputReferenceVisitor : RelationVisitor<Relation, bool>
    {
        private int _currentRelation;
        private bool _changeFound;
        private int _referenceIdCounter;
        private Dictionary<int, Dictionary<string, bool>> _relationReferences = new();
        private readonly Plan _plan;

        public WatermarkOutputReferenceVisitor(Plan plan)
        {
            _referenceIdCounter = 0;
            this._plan = plan;
        }

        public bool[] Run()
        {
            RunIteration();

            // We keep on running iterations until no changes are found
            while (_changeFound)
            {
                RunIteration();
            }

            var output = new bool[_plan.Relations.Count];
            for (int i = 0; i < _plan.Relations.Count; i++)
            {
                if (_relationReferences.TryGetValue(i, out var map))
                {
                    output[i] = map.Values.All(v => v);
                }
                else
                {
                    output[i] = false;
                }
            }
            return output;
        }

        private void RunIteration()
        {
            _changeFound = false;
            for (int i = 0; i < _plan.Relations.Count; i++)
            {
                RunRelationTree(i);
            }
        }

        public void RunRelationTree(int id)
        {
            _currentRelation = id;

            bool canUseEveryBatch = false;
            if (_relationReferences.TryGetValue(id, out var map))
            {
                if (map.Count > 0)
                {
                    canUseEveryBatch = true;
                }
                foreach (var kv in map)
                {
                    canUseEveryBatch &= kv.Value;
                }
            }
            _plan.Relations[id].Accept(this, canUseEveryBatch);
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, bool state)
        {
            filterRelation.Input = Visit(filterRelation.Input, state);
            return filterRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, bool state)
        {
            if (joinRelation.Type == JoinType.Left)
            {
                Visit(joinRelation.Left, state);
                Visit(joinRelation.Right, false);
            }
            else if (joinRelation.Type == JoinType.Right)
            {
                Visit(joinRelation.Right, state);
                Visit(joinRelation.Left, false);
            }
            else if (joinRelation.Type == JoinType.Inner)
            {
                Visit(joinRelation.Left, state);
                Visit(joinRelation.Right, false);
            }
            else
            {
                Visit(joinRelation.Left, false);
                Visit(joinRelation.Right, false);
            }
            
            return joinRelation;
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, bool state)
        {
            normalizationRelation.Input = Visit(normalizationRelation.Input, state);
            return normalizationRelation;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, bool state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            return projectRelation;
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, bool state)
        {
            writeRelation.Input = Visit(writeRelation.Input, true);
            return writeRelation;
        }

        public override Relation VisitPlanRelation(PlanRelation planRelation, bool state)
        {
            return planRelation;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, bool state)
        {
            return readRelation;
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, bool state)
        {
            // Use optimization property to temporarily add an identifier to each reference relation
            // This helps track which relations can run every batch
            // This property is removed later with a different visitor
            if (!referenceRelation.Hint.Optimizations.Properties.TryGetValue(WatermarkOutputUtils.ReferenceRelationPropertyName, out var id))
            {
                var newId = _referenceIdCounter++;
                id = newId.ToString();
                referenceRelation.Hint.Optimizations.Properties[WatermarkOutputUtils.ReferenceRelationPropertyName] = id;
                _changeFound = true;
            }

            if (!_relationReferences.TryGetValue(referenceRelation.RelationId, out var referenceMap))
            {
                referenceMap = new();
                _relationReferences[referenceRelation.RelationId] = referenceMap;
                _changeFound = true;
            }

            if (!referenceMap.TryGetValue(id, out var canRunEveryBatch))
            {
                referenceMap[id] = state;
                _changeFound = true;
            }

            if (canRunEveryBatch != state)
            {
                _changeFound = true;
                referenceMap[id] = state;
            }

            return referenceRelation;
        }

        public override Relation VisitRootRelation(RootRelation rootRelation, bool state)
        {
            rootRelation.Input = Visit(rootRelation.Input, state);
            return rootRelation;
        }

        public override Relation VisitSetRelation(SetRelation setRelation, bool state)
        {
            if (setRelation.Operation == SetOperation.UnionAll || setRelation.Operation == SetOperation.UnionDistinct)
            {
                for (int i = 0; i < setRelation.Inputs.Count; i++)
                {
                    setRelation.Inputs[i] = Visit(setRelation.Inputs[i], state);
                }
            }
            else
            {
                for (int i = 0; i < setRelation.Inputs.Count; i++)
                {
                    setRelation.Inputs[i] = Visit(setRelation.Inputs[i], false);
                }
            }

            return setRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, bool state)
        {
            if (mergeJoinRelation.Type == JoinType.Left)
            {
                Visit(mergeJoinRelation.Left, state);
                Visit(mergeJoinRelation.Right, false);
            }
            else if (mergeJoinRelation.Type == JoinType.Right)
            {
                Visit(mergeJoinRelation.Right, state);
                Visit(mergeJoinRelation.Left, false);
            }
            else if (mergeJoinRelation.Type == JoinType.Inner)
            {
                Visit(mergeJoinRelation.Left, state);
                Visit(mergeJoinRelation.Right, false);
            }
            else
            {
                Visit(mergeJoinRelation.Left, false);
                Visit(mergeJoinRelation.Right, false);
            }
            return mergeJoinRelation;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, bool state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, false);
            return aggregateRelation;
        }

        public override Relation VisitIterationRelation(IterationRelation iterationRelation, bool state)
        {
            if (iterationRelation.Input != null)
            {
                iterationRelation.Input = Visit(iterationRelation.Input, false);
            }
            iterationRelation.LoopPlan = Visit(iterationRelation.LoopPlan, false);
            return iterationRelation;
        }

        public override Relation VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, bool state)
        {
            return iterationReferenceReadRelation;
        }

        public override Relation VisitBufferRelation(BufferRelation bufferRelation, bool state)
        {
            bufferRelation.Input = Visit(bufferRelation.Input, state);
            return bufferRelation;
        }

        public override Relation VisitFetchRelation(FetchRelation fetchRelation, bool state)
        {
            fetchRelation.Input = Visit(fetchRelation.Input, false);
            return fetchRelation;
        }

        public override Relation VisitSortRelation(SortRelation sortRelation, bool state)
        {
            sortRelation.Input = Visit(sortRelation.Input, false);
            return sortRelation;
        }

        public override Relation VisitTopNRelation(TopNRelation topNRelation, bool state)
        {
            topNRelation.Input = Visit(topNRelation.Input, false);
            return topNRelation;
        }

        public override Relation VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, bool state)
        {
            if (tableFunctionRelation.Input != null)
            {
                tableFunctionRelation.Input = Visit(tableFunctionRelation.Input, false);
            }

            return tableFunctionRelation;
        }

        public override Relation VisitExchangeRelation(ExchangeRelation exchangeRelation, bool state)
        {
            exchangeRelation.Input = Visit(exchangeRelation.Input, state);
            return exchangeRelation;
        }

        public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, bool state)
        {
            return standardOutputExchangeReferenceRelation;
        }

        public override Relation VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, bool state)
        {
            return virtualTableReadRelation;
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, bool state)
        {
            consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, false);
            return consistentPartitionWindowRelation;
        }

        public override Relation VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, bool state)
        {
            subStreamRootRelation.Input = Visit(subStreamRootRelation.Input, false);
            return subStreamRootRelation;
        }

        public override Relation VisitPullExchangeReferenceRelation(PullExchangeReferenceRelation pullExchangeReferenceRelation, bool state)
        {
            return pullExchangeReferenceRelation;
        }

        public override Relation VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, bool state)
        {
            return substreamExchangeReferenceRelation;
        }
    }
}
