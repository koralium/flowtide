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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Base.Vertices.PartitionVertices;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Aggregate;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Core.Operators.Buffer;
using FlowtideDotNet.Core.Operators.Filter;
using FlowtideDotNet.Core.Operators.Iteration;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Join.NestedLoopJoin;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Core.Operators.Partition;
using FlowtideDotNet.Core.Operators.Project;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Operators.TableFunction;
using FlowtideDotNet.Core.Operators.TimestampProvider;
using FlowtideDotNet.Core.Operators.TopN;
using FlowtideDotNet.Core.Operators.VirtualTable;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Core.Operators.Exchange;

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
        private readonly IConnectorManager? connectorManager;
        private readonly IReadWriteFactory? readWriteFactory;
        private readonly FunctionsRegister functionsRegister;
        private readonly int parallelism;
        private readonly TimeSpan getTimestampInterval;
        private readonly bool _useColumnStore;
        private int _operatorId = 0;
        private Dictionary<int, RelationTree> _doneRelations;
        private Dictionary<string, ColumnIterationOperator> _iterationOperators = new Dictionary<string, ColumnIterationOperator>();
        private readonly TaskScheduler? _taskScheduler;
        private readonly DistributedOptions? _distributedOptions;
        private readonly int _queueSize;
        private readonly SubstreamCommunicationPointFactory? _communicationPointFactory;

        private ExecutionDataflowBlockOptions DefaultBlockOptions
        {
            get
            {
                var options = new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = _queueSize,
                    MaxDegreeOfParallelism = 1
                };
                if (_taskScheduler != null)
                {
                    options.TaskScheduler = _taskScheduler;
                }
                return options;
            }
        }

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
            IConnectorManager? connectorManager,
            IReadWriteFactory? readWriteFactory,
            int queueSize,
            FunctionsRegister functionsRegister,
            int parallelism,
            TimeSpan getTimestampInterval,
            bool useColumnStore,
            TaskScheduler? taskScheduler = default,
            DistributedOptions? distributedOptions = default)
        {
            this.plan = plan;
            this.dataflowStreamBuilder = dataflowStreamBuilder;
            this.connectorManager = connectorManager;
            this.readWriteFactory = readWriteFactory;
            this.functionsRegister = functionsRegister;
            this.parallelism = parallelism;
            this.getTimestampInterval = getTimestampInterval;
            _useColumnStore = useColumnStore;
            _queueSize = queueSize;
            _taskScheduler = taskScheduler;
            _distributedOptions = distributedOptions;
            _doneRelations = new Dictionary<int, RelationTree>();
            if (distributedOptions != null && distributedOptions.CommunicationHandlerFactory != null)
            {
                _communicationPointFactory = new SubstreamCommunicationPointFactory(distributedOptions.SubstreamName, distributedOptions.CommunicationHandlerFactory);
            }
        }

        //private ExecutionDataflowBlockOptions CreateBlockOptions()
        //{
        //    var options = new ExecutionDataflowBlockOptions()
        //    {
        //        BoundedCapacity = _queueSize,
        //        MaxDegreeOfParallelism = 1
        //    };
        //    if (taskScheduler != null)
        //    {
        //        options.TaskScheduler = taskScheduler;
        //    }
        //}

        public override IStreamVertex VisitFilterRelation(FilterRelation filterRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            UnaryVertex<StreamEventBatch>? op;

            if (_useColumnStore)
            {
                op = new ColumnFilterOperator(filterRelation, functionsRegister, DefaultBlockOptions);
            }
            else
            {
                op = new FilterOperator(filterRelation, functionsRegister, DefaultBlockOptions);
            }

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

            UnaryVertex<StreamEventBatch>? op;

            if (_useColumnStore)
            {
                op = new ColumnProjectOperator(projectRelation, functionsRegister, DefaultBlockOptions);
            }
            else
            {
                op = new ProjectOperator(projectRelation, functionsRegister, DefaultBlockOptions);
            }

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
                var partitionOperator = new PartitionOperator(new PartitionOperatorOptions(aggregateRelation.Groupings[0].GroupingExpressions), functionsRegister, parallelism, DefaultBlockOptions);
                dataflowStreamBuilder.AddPropagatorBlock(partitionOperatorId.ToString(), partitionOperator);

                var partitionCombinerId = _operatorId++;
                var partitionCombiner = new PartitionedOutputVertex<StreamEventBatch>(parallelism, DefaultBlockOptions);
                dataflowStreamBuilder.AddPropagatorBlock(partitionCombinerId.ToString(), partitionCombiner);

                for (int i = 0; i < parallelism; i++)
                {
                    var aggregateOperatorId = _operatorId++;
                    var aggregateOperator = new AggregateOperator(aggregateRelation, functionsRegister, DefaultBlockOptions);
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

                UnaryVertex<StreamEventBatch>? op;
                if (_useColumnStore)
                {
                    op = new ColumnAggregateOperator(aggregateRelation, functionsRegister, DefaultBlockOptions);
                }
                else
                {
                    op = new AggregateOperator(aggregateRelation, functionsRegister, DefaultBlockOptions);
                }

                if (state != null)
                {
                    op.LinkTo(state);
                }

                aggregateRelation.Input.Accept(this, op);
                dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
                return op;
            }
        }

        public override IStreamVertex VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, ITargetBlock<IStreamEvent>? state)
        {
            if (parallelism > 1)
            {
                var leftPartitionOperatorId = _operatorId++;
                var leftPartitionOperator = new PartitionOperator(new PartitionOperatorOptions(mergeJoinRelation.LeftKeys.ToList<Substrait.Expressions.Expression>()), functionsRegister, parallelism, DefaultBlockOptions);
                dataflowStreamBuilder.AddPropagatorBlock(leftPartitionOperatorId.ToString(), leftPartitionOperator);
                var rightPartitionOperatorId = _operatorId++;
                var rightPartitionOperator = new PartitionOperator(new PartitionOperatorOptions(mergeJoinRelation.RightKeys.Select(x =>
                {
                    if (x is DirectFieldReference direct && direct.ReferenceSegment is StructReferenceSegment structReference)
                    {
                        return new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = structReference.Field - mergeJoinRelation.Left.OutputLength,
                            }
                        };
                    }
                    return x;
                }).ToList<Substrait.Expressions.Expression>()), functionsRegister, parallelism, DefaultBlockOptions);
                dataflowStreamBuilder.AddPropagatorBlock(rightPartitionOperatorId.ToString(), rightPartitionOperator);

                var partitionCombinerId = _operatorId++;
                var partitionCombiner = new PartitionedOutputVertex<StreamEventBatch>(parallelism, DefaultBlockOptions);
                dataflowStreamBuilder.AddPropagatorBlock(partitionCombinerId.ToString(), partitionCombiner);

                for (int i = 0; i < parallelism; i++)
                {
                    var id = _operatorId++;
                    MultipleInputVertex<StreamEventBatch> op;
                    if (_useColumnStore)
                    {
                        op = new ColumnStoreMergeJoin(mergeJoinRelation, functionsRegister, DefaultBlockOptions);
                    }
                    else
                    {
                        op = new MergeJoinOperatorBase(mergeJoinRelation, functionsRegister, DefaultBlockOptions);
                    }
                    leftPartitionOperator.Sources[i].LinkTo(op.Targets[0]);
                    rightPartitionOperator.Sources[i].LinkTo(op.Targets[1]);
                    op.LinkTo(partitionCombiner.Targets[i]);
                    dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
                }

                if (state != null)
                {
                    partitionCombiner.LinkTo(state);
                }

                mergeJoinRelation.Left.Accept(this, leftPartitionOperator);
                mergeJoinRelation.Right.Accept(this, rightPartitionOperator);
                return partitionCombiner;
            }
            else
            {
                var id = _operatorId++;

                MultipleInputVertex<StreamEventBatch> op;
                if (_useColumnStore)
                {
                    op = new ColumnStoreMergeJoin(mergeJoinRelation, functionsRegister, DefaultBlockOptions);
                }
                else
                {
                    op = new MergeJoinOperatorBase(mergeJoinRelation, functionsRegister, DefaultBlockOptions);
                }

                if (state != null)
                {
                    op.LinkTo(state);
                }

                mergeJoinRelation.Left.Accept(this, op.Targets[0]);
                mergeJoinRelation.Right.Accept(this, op.Targets[1]);
                dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);

                return op;
            }
        }

        public override IStreamVertex VisitJoinRelation(JoinRelation joinRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;

            if (joinRelation.Type == JoinType.Left || joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Right || joinRelation.Type == JoinType.Outer)
            {
                //throw new NotSupportedException();
                var op = new BlockNestedJoinOperator(joinRelation, functionsRegister, DefaultBlockOptions);

                if (state != null)
                {
                    op.LinkTo(state, new DataflowLinkOptions()
                    {
                        PropagateCompletion = true
                    });
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
            var op = ColumnSetOperatorFactory.CreateColumnSetOperator(setRelation, DefaultBlockOptions); // new SetOperator(setRelation, DefaultBlockOptions);

            if (state != null)
            {
                op.LinkTo(state);
            }

            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                setRelation.Inputs[i].Accept(this, op.Targets[i]);
            }
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        private IStreamVertex VisitGetTimestampTable(ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new TimestampProviderOperator(TimeProvider.System, getTimestampInterval, DefaultBlockOptions);
            if (op is ISourceBlock<IStreamEvent> sourceBlock)
            {
                if (state != null)
                {
                    sourceBlock.LinkTo(state);
                }
            }
            else
            {
                throw new NotSupportedException("Read relation operator must implement ISourceBlock<IStreamEvent>");
            }
            dataflowStreamBuilder.AddIngressBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitReadRelation(ReadRelation readRelation, ITargetBlock<IStreamEvent>? state)
        {
            if (readRelation.NamedTable.DotSeperated == "__gettimestamp")
            {
                return VisitGetTimestampTable(state);
            }
            IStreamIngressVertex? op;
            ITargetBlock<IStreamEvent>? previousState;
            // TODO: Remove this if statement after readwritefactory is removed
            if (connectorManager != null)
            {
                var sourceFactory = connectorManager.GetSourceFactory(readRelation);
                op = sourceFactory.CreateSource(readRelation, functionsRegister, DefaultBlockOptions);
                previousState = state;
            }
            #region ReadwriteFactory obsolete
            else if (readWriteFactory != null)
            {
                var info = readWriteFactory.GetReadOperator(readRelation, functionsRegister, DefaultBlockOptions);

                previousState = state;
                if (info.NormalizationRelation != null)
                {
                    var normId = _operatorId++;
                    UnaryVertex<StreamEventBatch> normOp;
                    if (_useColumnStore)
                    {
                        normOp = new ColumnNormalizationOperator(info.NormalizationRelation, functionsRegister, DefaultBlockOptions);
                    }
                    else
                    {
                        normOp = new NormalizationOperator(info.NormalizationRelation, functionsRegister, DefaultBlockOptions);
                    }

                    if (state != null)
                    {
                        normOp.LinkTo(state);
                    }
                    previousState = normOp;
                    dataflowStreamBuilder.AddPropagatorBlock(normId.ToString(), normOp);
                }
                op = info.IngressVertex;
            }
            #endregion
            else
            {
                throw new InvalidOperationException("No ConnectorManager or ReadWriteFactory");
            }


            var id = _operatorId++;
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

            IStreamEgressVertex? op = default;
            if (connectorManager != null)
            {
                var sinkFactory = connectorManager.GetSinkFactory(writeRelation);
                op = sinkFactory.CreateSink(writeRelation, functionsRegister, DefaultBlockOptions);
            }
            else if (readWriteFactory != null)
            {
                op = readWriteFactory.GetWriteOperator(writeRelation, functionsRegister, DefaultBlockOptions);
            }
            else
            {
                throw new InvalidOperationException("No ConnectorManager or ReadWriteFactory");
            }

            if (op is ITargetBlock<IStreamEvent> target)
            {
                writeRelation.Input.Accept(this, target);
            }
            else
            {
                throw new NotSupportedException("Write operator must be an ITargetBlock<IStreamEvent>");
            }

            dataflowStreamBuilder.AddEgressBlock(id.ToString(), op);
            return op;
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
            UnaryVertex<StreamEventBatch> op;
            if (_useColumnStore)
            {
                op = new ColumnNormalizationOperator(normalizationRelation, functionsRegister, DefaultBlockOptions);
            }
            else
            {
                op = new NormalizationOperator(normalizationRelation, functionsRegister, DefaultBlockOptions);
            }

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
            VirtualTableOperator op = new VirtualTableOperator(virtualTableReadRelation, functionsRegister, DefaultBlockOptions);

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

        public override IStreamVertex VisitIterationRelation(IterationRelation iterationRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new ColumnIterationOperator(iterationRelation, functionsRegister, DefaultBlockOptions);
            if (state != null)
            {
                op.EgressSource.LinkTo(state);
            }
            if (iterationRelation.Input == null)
            {
                var ingressId = _operatorId++;
                var readDummy = new IterationDummyRead(DefaultBlockOptions);
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

        public override IStreamVertex VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, ITargetBlock<IStreamEvent>? state)
        {
            if (_iterationOperators.TryGetValue(iterationReferenceReadRelation.IterationName, out var op))
            {
                if (state != null)
                {
                    op.LoopSource.LinkTo(state);
                }
                return op;
            }
            throw new InvalidOperationException($"Iteration operator {iterationReferenceReadRelation.IterationName} not found");
        }

        public override IStreamVertex VisitBufferRelation(BufferRelation bufferRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new BufferOperator(bufferRelation, DefaultBlockOptions);
            if (state != null)
            {
                op.LinkTo(state);
            }
            bufferRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitTopNRelation(TopNRelation topNRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new TopNOperator(topNRelation, functionsRegister, DefaultBlockOptions);
            if (state != null)
            {
                op.LinkTo(state);
            }
            topNRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitFetchRelation(FetchRelation fetchRelation, ITargetBlock<IStreamEvent>? state)
        {
            throw new NotSupportedException("Fetch operation (top or limit) is not supported without an order by");
        }

        public override IStreamVertex VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;

            if (tableFunctionRelation.Input == null)
            {
                // Used as a root for data such as in FROM func()
                var op = new TableFunctionReadOperator(tableFunctionRelation, functionsRegister, DefaultBlockOptions);
                if (state != null)
                {
                    op.LinkTo(state);
                }

                dataflowStreamBuilder.AddIngressBlock(id.ToString(), op);
                return op;
            }
            else
            {
                // Used in a join or similar
                var op = new TableFunctionJoinOperator(tableFunctionRelation, functionsRegister, DefaultBlockOptions);

                if (state != null)
                {
                    op.LinkTo(state);
                }

                tableFunctionRelation.Input.Accept(this, op);
                dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
                return op;
            }
        }

        public override IStreamVertex VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new WindowOperator(consistentPartitionWindowRelation, functionsRegister, DefaultBlockOptions);
            if (state != null)
            {
                op.LinkTo(state);
            }
            consistentPartitionWindowRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddPropagatorBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitExchangeRelation(ExchangeRelation exchangeRelation, ITargetBlock<IStreamEvent>? state)
        {
            var id = _operatorId++;
            var op = new ExchangeOperator(exchangeRelation, _communicationPointFactory, functionsRegister, DefaultBlockOptions);

            exchangeRelation.Input.Accept(this, op);
            dataflowStreamBuilder.AddEgressBlock(id.ToString(), op);
            return op;
        }

        /// <summary>
        /// Links exchange operators to their standard output, no special operator is required.
        /// It just links the partition output the the linking block.
        /// </summary>
        /// <param name="standardOutputExchangeReferenceRelation"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public override IStreamVertex VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, ITargetBlock<IStreamEvent>? state)
        {
            // Fetch exchange 
            var relTree = GetOrBuildRelation(standardOutputExchangeReferenceRelation.RelationId);
            if (relTree.StreamVertex is ExchangeOperator exchangeOperator)
            {
                if (state != null)
                {
                    int sourceTargetId = 0;
                    for (int i = 0; i < exchangeOperator.exchangeRelation.Targets.Count && i < standardOutputExchangeReferenceRelation.TargetId; i++)
                    {
                        var target = exchangeOperator.exchangeRelation.Targets[i];
                        if (target.Type == ExchangeTargetType.StandardOutput)
                        {
                            sourceTargetId++;
                        }
                    }
                    exchangeOperator.Sources[sourceTargetId].LinkTo(state);
                }
                return exchangeOperator;
            }
            else
            {
                throw new InvalidOperationException("StandardOutputExchangeReferenceRelation must reference an ExchangeOperator");
            }
        }

        public override IStreamVertex VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, ITargetBlock<IStreamEvent>? state)
        {
            if (_distributedOptions == null)
            {
                return Visit(subStreamRootRelation.Input, state);
            }
            else
            {
                if (_distributedOptions.SubstreamName == subStreamRootRelation.Name)
                {
                    return Visit(subStreamRootRelation.Input, state);
                }
                return null;
            }
        }

        public override IStreamVertex VisitPullExchangeReferenceRelation(PullExchangeReferenceRelation pullExchangeReferenceRelation, ITargetBlock<IStreamEvent> state)
        {
            if(_distributedOptions == null)
            {
                throw new InvalidOperationException("PullExchangeReferenceRelation is not supported without DistributedOptions");
            }
            var op = _distributedOptions.PullBucketExchangeReadFactory.GetOperator(pullExchangeReferenceRelation, DefaultBlockOptions);
            var id = _operatorId++;
            if (state != null && op is ISourceBlock<IStreamEvent> sourceBlock)
            {
                sourceBlock.LinkTo(state);
            }
            dataflowStreamBuilder.AddIngressBlock(id.ToString(), op);
            return op;
        }

        public override IStreamVertex VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, ITargetBlock<IStreamEvent>? state)
        {
            if (_distributedOptions == null)
            {
                throw new InvalidOperationException("SubstreamExchangeReferenceRelation is not supported without DistributedOptions");
            }
            

            var comPoint = _communicationPointFactory.GetCommunicationPoint(substreamExchangeReferenceRelation.SubStreamName);
            var op = new SubstreamReadOperator(comPoint, substreamExchangeReferenceRelation, DefaultBlockOptions);
            var id = _operatorId++;
            if (state != null && op is ISourceBlock<IStreamEvent> sourceBlock)
            {
                sourceBlock.LinkTo(state);
            }
            dataflowStreamBuilder.AddIngressBlock(id.ToString(), op);
            return op;
        }
    }
}
