﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Operators.Filter;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Join.NestedLoopJoin;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Core.Operators.Project;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Operators.Unwrap;
using FlowtideDotNet.Core.Operators.VirtualTable;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Aggregate;
using FlowtideDotNet.Core.Operators.Iteration;
using FlowtideDotNet.Core.Operators.Partition;
using FlowtideDotNet.Base.Vertices.PartitionVertices;

namespace FlowtideDotNet.Core.Engine
{
    internal class SubstraitVisitor : RelationVisitor<IStreamVertex, ITargetBlock<IStreamEvent>?>
    {
        private class RelationTree
        {
            public RelationTree(ISourceBlock<IStreamEvent>? sourceBlock, IStreamVertex streamVertex)
            {
                SourceBlock = sourceBlock;
                StreamVertex = streamVertex;
            }

            public ISourceBlock<IStreamEvent>? SourceBlock { get; }

            public IStreamVertex StreamVertex { get; }
        }

        private readonly Plan plan;
        private readonly DataflowStreamBuilder dataflowStreamBuilder;
        private readonly IReadWriteFactory readFactory;
        private readonly int queueSize;
        private readonly FunctionsRegister functionsRegister;
        private readonly int parallelism;
        private int _operatorId = 0;
        private Dictionary<int, RelationTree> _doneRelations;
        private Dictionary<string, IterationOperator> _iterationOperators = new Dictionary<string, IterationOperator>();

        public void BuildPlan()
        {
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                GetOrBuildRelation(i);
            }
        }

        private RelationTree GetOrBuildRelation(int index)
        {
            if (index >= plan.Relations.Count)
            {
                throw new NotSupportedException("Relation index is higher than the number of relations.");
            }
            if (_doneRelations.TryGetValue(index, out var tree))
            {
                return tree;
            }
            var block = Visit(plan.Relations[index], null);

            RelationTree? relationTree = null;
            if (block is ISourceBlock<IStreamEvent> sourceBlock)
            {
                relationTree = new RelationTree(sourceBlock, block);
                _doneRelations.Add(index, relationTree);
            }
            else
            {
                relationTree = new RelationTree(null, block);
                _doneRelations.Add(index, relationTree);
            }
            return relationTree;
        } 

        public SubstraitVisitor(
            Plan plan, 
            DataflowStreamBuilder dataflowStreamBuilder, 
            IReadWriteFactory readFactory, 
            int queueSize, 
            FunctionsRegister functionsRegister,
            int parallelism)
        {
            this.plan = plan;
            this.dataflowStreamBuilder = dataflowStreamBuilder;
            this.readFactory = readFactory;
            this.queueSize = queueSize;
            this.functionsRegister = functionsRegister;
            this.parallelism = parallelism;
            _doneRelations = new Dictionary<int, RelationTree>();
        }

        public override IStreamVertex VisitFilterRelation(FilterRelation filterRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new FilterOperator(filterRelation, functionsRegister, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = queueSize,
                MaxDegreeOfParallelism = 1
            });

            if (state != null)
            {
                op.LinkTo(state);
            }
            
            filterRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitProjectRelation(ProjectRelation projectRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new ProjectOperator(projectRelation, functionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1});

            if (state != null)
            {
                op.LinkTo(state);
            }
            
            projectRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitAggregateRelation(AggregateRelation aggregateRelation, ITargetBlock<IStreamEvent>? state)
        {
            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count == 1 && parallelism > 1)
            {
                
                var partitionOperatorId = _operatorId++;
                var partitionOperator = new PartitionOperator(new PartitionOperatorOptions(aggregateRelation.Groupings[0].GroupingExpressions), functionsRegister, parallelism, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = 100,
                    MaxDegreeOfParallelism = 1
                });
                dataflowStreamBuilder.AddPropagatorBlock(partitionOperatorId.ToString(), partitionOperator);

                var partitionCombinerId = _operatorId++;
                var partitionCombiner = new PartitionedOutputVertex<StreamEventBatch, object>(parallelism, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });
                dataflowStreamBuilder.AddPropagatorBlock(partitionCombinerId.ToString(), partitionCombiner);

                for (int i = 0; i < parallelism; i++)
                {
                    var aggregateOperatorId = _operatorId++;
                    var aggregateOperator = new AggregateOperator(aggregateRelation, functionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });
                    // Link partition output to the aggregate operator
                    partitionOperator.Sources[i].LinkTo(aggregateOperator);
                    dataflowStreamBuilder.AddPropagatorBlock(aggregateOperatorId.ToString(), aggregateOperator);

                    // Link aggregate output to the combiner
                    aggregateOperator.LinkTo(partitionCombiner.Targets[i]);
                }
                if (state != null)
                {
                    partitionCombiner.LinkTo(state);
                }
                aggregateRelation.Input.Accept(this, partitionOperator);
                return partitionOperator;
            }
            else
            {
                var id = _operatorId++;
                var op = new AggregateOperator(aggregateRelation, functionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });

                if (state != null)
                {
                    op.LinkTo(state);
                }

                aggregateRelation.Input.Accept(this, op);
                dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
                return op;
            }
        }

        public override IStreamVertex VisitUnwrapRelation(UnwrapRelation unwrapRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;

            var op = new UnwrapOperator(unwrapRelation, functionsRegister, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = queueSize,
                MaxDegreeOfParallelism = 1
            });

            if (state != null)
            {
                op.LinkTo(state);
            }

            unwrapRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);

            return op;
        }

        public override IStreamVertex VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;

            var op = new MergeJoinOperatorBase(mergeJoinRelation, functionsRegister, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = queueSize,
                MaxDegreeOfParallelism = 1
            });
            if (state != null)
            {
                op.LinkTo(state);
            }

            mergeJoinRelation.Left.Accept(this, op.Targets[0]);
            mergeJoinRelation.Right.Accept(this, op.Targets[1]);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);

            return op;
        }

        public override IStreamVertex VisitJoinRelation(JoinRelation joinRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            
            if (joinRelation.Type == JoinType.Left || joinRelation.Type == JoinType.Inner)
            {
                //throw new NotSupportedException();
                var op = new BlockNestedJoinOperator(joinRelation, functionsRegister, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = queueSize,
                    MaxDegreeOfParallelism = 1
                });

                if (state != null)
                {
                    op.LinkTo(state);
                }

                joinRelation.Left.Accept(this, op.Targets[0]);
                joinRelation.Right.Accept(this, op.Targets[1]);
                dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);

                return op;
            }
            else
            {
                throw new NotSupportedException("Join type is not yet supported");
            }
        }

        public override IStreamVertex VisitSetRelation(SetRelation setRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new SetOperator(setRelation, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = queueSize,
                MaxDegreeOfParallelism = 1
            });
            
            if (state != null)
            {
                op.LinkTo(state);
            }
            
            for(int i = 0; i < setRelation.Inputs.Count; i++)
            {
                setRelation.Inputs[i].Accept(this, op.Targets[i]);
            }
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitReadRelation(ReadRelation readRelation, ITargetBlock<IStreamEvent>? state)
        {
            var info = readFactory.GetReadOperator(readRelation, new DataflowBlockOptions() { BoundedCapacity = queueSize });

            var previousState = state;
            if (info.NormalizationRelation != null)
            {
                var normId = _operatorId++;
                NormalizationOperator normOp = new NormalizationOperator(info.NormalizationRelation, functionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });

                if (state != null)
                {
                    normOp.LinkTo(state);
                }
                previousState = normOp;
                dataflowStreamBuilder.AddPropagatorBlock(normId.ToString(), normOp);
            }

            var id = _operatorId++;
            var op = info.IngressVertex;
            if (op is ISourceBlock<IStreamEvent> sourceBlock)
            {
                if (previousState != null)
                {
                    sourceBlock.LinkTo(previousState);
                }
            }
            else
            {
                throw new NotSupportedException("Read relation operator must implement ISourceBlock<IStreamEvent>");
            }
            dataflowStreamBuilder.AddIngressBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitWriteRelation(WriteRelation writeRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = readFactory.GetWriteOperator(writeRelation, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });

            if (op is ITargetBlock<IStreamEvent> target)
            {
                writeRelation.Input.Accept(this, target);
            }
            else
            {
                throw new NotSupportedException("Write operator must be an ITargetBlock<IStreamEvent>");
            }
            
            dataflowStreamBuilder.AddEgressBlock(id.ToString(), op);
            return base.VisitWriteRelation(writeRelation, state);
        }

        /// <summary>
        /// Handles reference relations in the DAG. It builds the relation if its not already created, and connects the output to the node
        /// that references it.
        /// </summary>
        /// <param name="referenceRelation"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        public override IStreamVertex VisitReferenceRelation(ReferenceRelation referenceRelation, ITargetBlock<IStreamEvent>? state)
        {
            var relation = GetOrBuildRelation(referenceRelation.RelationId);

            if (state != null && relation.SourceBlock != null)
            {
                relation.SourceBlock.LinkTo(state);
            }

            return relation.StreamVertex;
        }

        public override IStreamVertex VisitNormalizationRelation(NormalizationRelation normalizationRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            NormalizationOperator op = new NormalizationOperator(normalizationRelation, functionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });

            if (state != null)
            {
                op.LinkTo(state);
            }

            normalizationRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            VirtualTableOperator op = new VirtualTableOperator(virtualTableReadRelation, new DataflowBlockOptions() { BoundedCapacity = queueSize });

            if (state != null)
            {
                op.LinkTo(state);
            }
            dataflowStreamBuilder.AddIngressBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitRootRelation(RootRelation rootRelation, ITargetBlock<IStreamEvent>? state)
        {
            return Visit(rootRelation.Input, state);
        }

        public override IStreamVertex VisitIterationRelation(IterationRelation iterationRelation, ITargetBlock<IStreamEvent> state)
        {
            var id = _operatorId++;
            var op = new IterationOperator(new ExecutionDataflowBlockOptions() { BoundedCapacity = queueSize, MaxDegreeOfParallelism = 1 });
            if (state != null)
            {
                op.EgressSource.LinkTo(state);
            }
            if (iterationRelation.Input == null)
            {
                var ingressId = _operatorId++;
                var readDummy = new IterationDummyRead(new DataflowBlockOptions() { BoundedCapacity = queueSize });
                readDummy.LinkTo(op.IngressTarget);
                dataflowStreamBuilder.AddIngressBlock(ingressId.ToString(), readDummy);
            }
            else
            {
                Visit(iterationRelation.Input, op.IngressTarget);
            }
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);

            _iterationOperators.Add(iterationRelation.IterationName, op);
            Visit(iterationRelation.LoopPlan, op.FeedbackTarget);
            
            return (op.EgressSource as IStreamVertex)!;
        }

        public override IStreamVertex VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, ITargetBlock<IStreamEvent> state)
        {
            if (_iterationOperators.TryGetValue(iterationReferenceReadRelation.IterationName, out var op))
            {
                op.LoopSource.LinkTo(state);
            }
            return op;
        }
    }
}
