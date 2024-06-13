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

using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Join.NestedLoopJoin;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class ColumnStoreMergeJoin : MultipleInputVertex<StreamEventBatch, JoinState>
    {
        protected IBPlusTree<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>? _leftTree;
        protected IBPlusTree<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>? _rightTree;

        private readonly MergeJoinRelation _mergeJoinRelation;
        private MergeJoinInsertComparer _leftInsertComparer;
        private MergeJoinInsertComparer _rightInsertComparer;
        private MergeJoinSearchComparer _searchLeftComparer;
        private MergeJoinSearchComparer _searchRightComparer;

        public ColumnStoreMergeJoin(MergeJoinRelation mergeJoinRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
            this._mergeJoinRelation = mergeJoinRelation;
            var leftColumns = GetCompareColumns(mergeJoinRelation.LeftKeys, 0);
            var rightColumns = GetCompareColumns(mergeJoinRelation.RightKeys, mergeJoinRelation.Left.OutputLength);
            _leftInsertComparer = new MergeJoinInsertComparer(leftColumns, mergeJoinRelation.Left.OutputLength);
            _rightInsertComparer = new MergeJoinInsertComparer(rightColumns, mergeJoinRelation.Right.OutputLength);

            _searchLeftComparer = new MergeJoinSearchComparer(leftColumns, rightColumns);
            _searchRightComparer = new MergeJoinSearchComparer(rightColumns, leftColumns);
        }

        private static List<int> GetCompareColumns(List<FieldReference> fieldReferences, int relativeIndex)
        {
            List<int> leftKeys = new List<int>();
            for (int i = 0; i < fieldReferences.Count; i++)
            {
                if (fieldReferences[i] is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    leftKeys.Add(structReferenceSegment.Field - relativeIndex);
                }
                else
                {
                    throw new NotImplementedException("Merge join can only have keys that use struct reference segments at this time");
                }
            }
            return leftKeys;
        }

        public override string DisplayName => "Merge Join";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<JoinState?> OnCheckpoint()
        {
            return Task.FromResult(new JoinState());
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveLeft(StreamEventBatch msg, long time)
        {
            var it = _rightTree!.CreateIterator();
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var columnReference = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await it.Seek(in columnReference, _searchRightComparer);
                int weight = msg.Data.Weights[i];
                int joinWeight = 0;
                if (!_searchRightComparer.noMatch)
                {
                    bool firstPage = true;
                    
                    await foreach(var page in it)
                    {
                        if (!firstPage)
                        {
                            // Locate indices again
                        }
                        // All in this range matched the key comparisons
                        for (int k = _searchRightComparer.start; k <= _searchRightComparer.end; k++)
                        {
                            int outputWeight = page.Values.Get(k).Weight * msg.Data.Weights[i];
                            joinWeight += outputWeight;
                        }
                        if (_searchRightComparer.end < (page.Keys.Count - 1))
                        {
                            break;
                        }
                    }
                }
                await _leftTree!.RMW(in columnReference, new JoinStorageValue() { Weight = weight, JoinWeight = joinWeight }, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.Weight += input!.Weight;
                        current.JoinWeight += input.JoinWeight;
                        if (current.Weight == 0)
                        {
                            return (default, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });


            }
            yield break;
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveRight(StreamEventBatch msg, long time)
        {
            var it = _leftTree!.CreateIterator();
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var columnReference = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await it.Seek(in columnReference, _searchLeftComparer);

                int weight = msg.Data.Weights[i];
                int joinWeight = 0;
                if (!_searchLeftComparer.noMatch)
                {
                
                    bool firstPage = true;
                    
                    await foreach (var page in it)
                    {
                        if (!firstPage)
                        {
                            // Locate indices again
                        }
                        // All in this range matched the key comparisons
                        for (int k = _searchLeftComparer.start; k <= _searchLeftComparer.end; k++)
                        {
                            int outputWeight = page.Values.Get(k).Weight * weight;
                            joinWeight += outputWeight;
                        }
                        if (_searchLeftComparer.end < (page.Keys.Count - 1))
                        {
                            break;
                        }
                    }
                }
                await _rightTree!.RMW(in columnReference, new JoinStorageValue() { Weight = weight, JoinWeight = joinWeight }, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.Weight += input!.Weight;
                        current.JoinWeight += input.JoinWeight;
                        if (current.Weight == 0)
                        {
                            return (default, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });


            }
            yield break;
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            if (targetId == 0)
            {
                return OnRecieveLeft(msg, time);
            }
            else
            {
                return OnRecieveRight(msg, time);
            }
        }

        protected override async Task InitializeOrRestore(JoinState? state, IStateManagerClient stateManagerClient)
        {
            _leftTree = await stateManagerClient.GetOrCreateTree("left",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>()
                {
                    Comparer = _leftInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Left.OutputLength),
                    ValueSerializer = new ValueListSerializer<JoinStorageValue>(new JoinStorageValueBPlusTreeSerializer())
                });
            _rightTree = await stateManagerClient.GetOrCreateTree("right",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>()
                {
                    Comparer = _rightInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Right.OutputLength),
                    ValueSerializer = new ValueListSerializer<JoinStorageValue>(new JoinStorageValueBPlusTreeSerializer())
                });
        }
    }
}
