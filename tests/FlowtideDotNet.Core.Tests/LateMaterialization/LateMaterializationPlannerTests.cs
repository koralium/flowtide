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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class LateMaterializationPlannerTests
{
    private sealed class NamedKeySourceFactory(IReadOnlyList<string>? keys) : RegexConnectorSourceFactory("*")
    {
        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new TwoColumnSource(readRelation, functionsRegister, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => keys;
    }

    private sealed class TwoColumnSource : ColumnBatchReadBaseOperator
    {
        public TwoColumnSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options)
            : base(readRelation, functionsRegister, options)
        {
        }

        public override string DisplayName => "two column source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 1 });

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            var allocator = GlobalMemoryManager.Instance;
            IColumn[] cols = [Column.Create(allocator), Column.Create(allocator)];
            cols[0].Add(new StringValue("wrong_text"));
            cols[1].Add(new Int64Value(42));
            yield return new ColumnReadEvent(
                new EventBatchWeighted(
                    new PrimitiveList<int>(allocator) { 1 },
                    new PrimitiveList<uint>(allocator) { 0 },
                    new EventBatchData(cols)),
                1);
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            await EnterCheckpointLock();
            try
            {
                var allocator = GlobalMemoryManager.Instance;
                IColumn[] empty = [Column.Create(allocator), Column.Create(allocator)];
                yield return new DeltaReadEvent(
                    new EventBatchWeighted(new PrimitiveList<int>(allocator), new PrimitiveList<uint>(allocator), new EventBatchData(empty)),
                    new Watermark("t", LongWatermarkValue.Create(1)));
            }
            finally
            {
                ExitCheckpointLock();
            }
        }
    }

    private class FourColumnSource : ColumnBatchReadBaseOperator
    {
        public FourColumnSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options)
            : base(readRelation, functionsRegister, options)
        {
        }

        public override string DisplayName => "four column source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            var allocator = GlobalMemoryManager.Instance;
            IColumn[] cols = [Column.Create(allocator), Column.Create(allocator), Column.Create(allocator), Column.Create(allocator)];
            cols[0].Add(new Int64Value(1));
            cols[1].Add(new Int64Value(10));
            cols[2].Add(new StringValue("hello"));
            cols[3].Add(new StringValue("world"));
            yield return new ColumnReadEvent(
                new EventBatchWeighted(
                    new PrimitiveList<int>(allocator) { 1 },
                    new PrimitiveList<uint>(allocator) { 0 },
                    new EventBatchData(cols)),
                1);
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            await EnterCheckpointLock();
            try
            {
                var allocator = GlobalMemoryManager.Instance;
                IColumn[] empty = [Column.Create(allocator), Column.Create(allocator), Column.Create(allocator), Column.Create(allocator)];
                yield return new DeltaReadEvent(
                    new EventBatchWeighted(new PrimitiveList<int>(allocator), new PrimitiveList<uint>(allocator), new EventBatchData(empty)),
                    new Watermark("t", LongWatermarkValue.Create(1)));
            }
            finally
            {
                ExitCheckpointLock();
            }
        }
    }

    private sealed class FourColumnSourceFactory : RegexConnectorSourceFactory
    {
        public FourColumnSourceFactory() : base("*")
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new FourColumnSource(readRelation, functionsRegister, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => ["k"];
    }

    private sealed class ConfigurableGroupedSink : ColumnGroupedWriteOperator
    {
        private readonly IReadOnlyList<int> _primaryKeys;
        private readonly TaskCompletionSource<List<string>> _uploaded;

        public ConfigurableGroupedSink(WriteRelation writeRelation, IReadOnlyList<int> primaryKeys, TaskCompletionSource<List<string>> uploaded, ExecutionDataflowBlockOptions options)
            : base(ExecutionMode.OnCheckpoint, writeRelation, options)
        {
            _primaryKeys = primaryKeys;
            _uploaded = uploaded;
        }

        public override string DisplayName => "configurable grouped sink";
        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(_primaryKeys);
        protected override void Checkpoint(long checkpointTime) { }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            var list = new List<string>();
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                if (!row.IsDeleted)
                {
                    var cols = row.EventBatchData.Columns;
                    var parts = new string[cols.Count];
                    for (int i = 0; i < cols.Count; i++)
                    {
                        parts[i] = cols[i].GetValueAt(row.Index, default).ToString()!;
                    }
                    list.Add(string.Join("|", parts));
                }
            }
            if (list.Count > 0)
            {
                _uploaded.TrySetResult(list);
            }
        }
    }

    private sealed class ConfigurableGroupedSinkFactory(IReadOnlyList<int> runtimeKeys, IReadOnlyList<int>? fetchKeys, TaskCompletionSource<List<string>> uploaded) : RegexConnectorSinkFactory("*")
    {
        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => new ConfigurableGroupedSink(writeRelation, runtimeKeys, uploaded, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation)
            => fetchKeys;
    }

    private sealed class CheckpointDoneListener(TaskCompletionSource done) : ICheckpointListener
    {
        public void OnCheckpointComplete(StreamCheckpointNotification notification) => done.TrySetResult();
    }

    private sealed class OccurrenceGroupedSinkFactory(string fetchingColumn) : RegexConnectorSinkFactory("*")
    {
        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => new ConfigurableGroupedSink(writeRelation, [0], new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously), dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation)
            => writeRelation.TableSchema.Names[0] == fetchingColumn ? [0] : null;
    }

    [Fact]
    public void ParallelizedJoinWithPostJoinFilterRewritesWithoutKeyConflict()
    {
        var left = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["l"] },
            BaseSchema = new NamedStruct
            {
                Names = ["k", "n", "text"],
                Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType()] }
            }
        };
        var right = Read("r", false);
        var join = new MergeJoinRelation
        {
            Left = left,
            Right = right,
            Type = JoinType.Inner,
            LeftKeys = [Field(1)],
            RightKeys = [Field(left.OutputLength)],
            PostJoinFilter = Compare(FunctionsComparison.NotEqual, Field(2), Field(left.OutputLength + 2))
        };
        var plan = ParallelizeOptimizer.Optimize(Write(join, 7), 2);
        var connectors = new ConnectorManager();
        connectors.AddSource(new FourColumnSourceFactory());
        var uploaded = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        connectors.AddSink(new ConfigurableGroupedSinkFactory([0], null, uploaded));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        int stores = LateMaterializationPass.Run(plan, connectors, functions, 1);
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        Assert.Equal(2, stores);
    }

    [Fact]
    public async Task SharedPlainProjectExpressionAcrossUnionLanesOutputsSameColumn()
    {
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["text", "k"],
                Struct = new Struct { Types = [new AnyType(), new AnyType()] }
            }
        };
        var sharedKey = Field(1);
        var lane0 = new ProjectRelation
        {
            Input = new ReferenceRelation { RelationId = 0, ReferenceOutputLength = 2 },
            Expressions = [sharedKey],
            Emit = [2]
        };
        var lane1 = new ProjectRelation
        {
            Input = new ReferenceRelation { RelationId = 0, ReferenceOutputLength = 2 },
            Expressions = [sharedKey],
            Emit = [2]
        };
        var union = new SetRelation
        {
            Operation = SetOperation.UnionAll,
            Inputs = [lane0, lane1]
        };
        var plan = Write(union, 1);
        plan.Relations.Insert(0, read);

        var sink = new DifferentialSink();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connectors = new ConnectorManager();
        connectors.AddSource(new NamedKeySourceFactory(["k"]));
        connectors.AddSink(new SimpleSinkFactory(sink));
        await using var stream = new FlowtideBuilder("shared-plain-project-red")
            .AddPlan(plan, false)
            .AddConnectorManager(connectors)
            .SetLateMaterialization(LateMaterializationMode.ForceOn)
            .WithCheckpointListener(new CheckpointDoneListener(done))
            .WithStateOptions(new StateManagerOptions
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
            })
            .Build();

        await stream.StartAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var rows = new Dictionary<string, int>();
        sink.AddTo(rows);
        Assert.Equal(new Dictionary<string, int> { ["42"] = 2 }, rows);
    }

    private sealed class SimpleSinkFactory(IStreamEgressVertex sink) : RegexConnectorSinkFactory("*")
    {
        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => sink;

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    [Fact]
    public async Task ExchangeWithEmittingReferenceBuildsUnderForceOn()
    {
        static Plan BuildExchangePlan()
        {
            var exchange = new ExchangeRelation
            {
                Input = Read("t", false),
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget { PartitionIds = [0] }]
            };
            var reference = new StandardOutputExchangeReferenceRelation
            {
                RelationId = 0,
                TargetId = 0,
                ReferenceOutputLength = 4,
                Emit = [0, 1, 2, 3]
            };
            var plan = Write(reference, 4);
            plan.Relations.Insert(0, exchange);
            return plan;
        }

        await using var pair = DifferentialPair.Planned(BuildExchangePlan, () => [Table("t", Row(1, 10, "a", "x"))]);
        await pair.StartAsync();
        await pair.Expect("1|10|a|x");
    }

    [Fact]
    public async Task SubStreamRootWrappingLocalExchangeBuildsUnderForceOn()
    {
        static Plan BuildSubStreamExchangePlan()
        {
            var exchange = new ExchangeRelation
            {
                Input = Read("t", false),
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget { PartitionIds = [0] }]
            };
            var reference = new StandardOutputExchangeReferenceRelation
            {
                RelationId = 0,
                TargetId = 0,
                ReferenceOutputLength = 4
            };
            var plan = Write(reference, 4);
            plan.Relations[0] = new SubStreamRootRelation { Name = "s0", Input = plan.Relations[0] };
            plan.Relations.Insert(0, new SubStreamRootRelation { Name = "s0", Input = exchange });
            return plan;
        }

        await using var pair = DifferentialPair.Planned(BuildSubStreamExchangePlan, () => [Table("t", Row(1, 10, "a", "x"))]);
        await pair.StartAsync();
        await pair.Expect("1|10|a|x");
    }

    [Fact]
    public async Task MultiJoinWithAliasedStoredColumnInCompositeKeyBuildsUnderForceOn()
    {
        static Plan BuildAliasedMultiJoinPlan()
        {
            var sub = new ProjectRelation
            {
                Input = Read("a", false),
                Expressions = [],
                Emit = [0, 2, 2, 3]
            };
            var first = new MergeJoinRelation
            {
                Left = sub,
                Right = Read("b", false),
                Type = JoinType.Left,
                LeftKeys = [Field(1), Field(2)],
                RightKeys = [Field(4), Field(6)]
            };
            var second = new MergeJoinRelation
            {
                Left = first,
                Right = Read("c", false),
                Type = JoinType.Left,
                LeftKeys = [Field(0)],
                RightKeys = [Field(8)]
            };
            var plan = Write(second, 12);
            Assert.Equal(1, MultiJoinMarker.Mark(plan, 8));
            return plan;
        }

        await using var pair = DifferentialPair.Planned(
            BuildAliasedMultiJoinPlan,
            () =>
            [
                Table("a", Row(1, 10, "same", "noteA")),
                Table("b", Row(2, 20, "same", "noteB")),
                Table("c", Row(1, 30, "c1", "noteC"))
            ]);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }

    [Fact]
    public async Task SinkDeclaringEmptyFetchKeyListDoesNotFuseFetchedRuntimeKey()
    {
        var uploaded = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var connectors = new ConnectorManager();
        connectors.AddSource(new FourColumnSourceFactory());
        connectors.AddSink(new ConfigurableGroupedSinkFactory([1], [], uploaded));
        await using var stream = new FlowtideBuilder("empty-fetch-keys-red")
            .AddPlan(Write(Read("t", false), 4), false)
            .AddConnectorManager(connectors)
            .SetLateMaterialization(LateMaterializationMode.ForceOn)
            .WithFailureListener(e => { if (e != null) failed.TrySetResult(e); })
            .WithStateOptions(new StateManagerOptions
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
            })
            .Build();

        await stream.StartAsync();
        var finished = await Task.WhenAny(uploaded.Task, failed.Task).WaitAsync(TimeSpan.FromSeconds(15));
        if (finished == failed.Task)
        {
            throw await failed.Task;
        }
        var rows = await uploaded.Task;
        Assert.Equal(["1|10|hello|world"], rows);
    }

    [Fact]
    public async Task SharedUnionRelationWithEmitOutputsSameStoredColumnFromBothBranches()
    {
        var sharedUnion = new SetRelation
        {
            Operation = SetOperation.UnionAll,
            Inputs = [new ReferenceRelation { RelationId = 0, ReferenceOutputLength = 4 }],
            Emit = [0, 2]
        };
        var outerUnion = new SetRelation
        {
            Operation = SetOperation.UnionAll,
            Inputs = [sharedUnion, sharedUnion]
        };
        var plan = Write(outerUnion, 2);
        plan.Relations.Insert(0, Read("t", false));

        var sink = new DifferentialSink();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connectors = new ConnectorManager();
        connectors.AddSource(new FourColumnSourceFactory());
        connectors.AddSink(new SimpleSinkFactory(sink));
        await using var stream = new FlowtideBuilder("shared-union-emit-red")
            .AddPlan(plan, false)
            .AddConnectorManager(connectors)
            .SetLateMaterialization(LateMaterializationMode.ForceOn)
            .WithCheckpointListener(new CheckpointDoneListener(done))
            .WithStateOptions(new StateManagerOptions
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
            })
            .Build();

        await stream.StartAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var rows = new Dictionary<string, int>();
        sink.AddTo(rows);
        Assert.Equal(new Dictionary<string, int> { ["1|hello"] = 2 }, rows);
    }

    [Fact]
    public void SharedExpressionInSingleBucketExchangeKeepsOriginalPlainFieldIndex()
    {
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["text", "k"],
                Struct = new Struct { Types = [new AnyType(), new AnyType()] }
            }
        };
        var sharedField = Field(1);
        var filter = new FilterRelation
        {
            Input = new ReferenceRelation { RelationId = 0, ReferenceOutputLength = 2 },
            Condition = Compare(FunctionsComparison.GreaterThan, sharedField, new Substrait.Expressions.Literals.NumericLiteral { Value = 0 })
        };
        var singleBucketExchange = new ExchangeRelation
        {
            Input = new ReferenceRelation { RelationId = 0, ReferenceOutputLength = 2 },
            ExchangeKind = new SingleBucketExchangeKind { Expression = sharedField },
            Targets = [new StandardOutputExchangeTarget { PartitionIds = [0] }]
        };
        var plan = new Plan
        {
            Relations =
            [
                read,
                singleBucketExchange,
                new WriteRelation
                {
                    Input = filter,
                    NamedObject = new NamedTable { Names = ["output"] },
                    TableSchema = new NamedStruct { Names = ["c0", "c1"] }
                }
            ]
        };
        var connectors = new ConnectorManager();
        connectors.AddSource(new NamedKeySourceFactory(["k"]));
        connectors.AddSink(new SimpleSinkFactory(new DifferentialSink()));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        LateMaterializationPass.Run(plan, connectors, functions, 1);
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        Assert.Equal(1, ((StructReferenceSegment)sharedField.ReferenceSegment).Field);
    }

    [Fact]
    public async Task EnablingSinkFetchRoleChangesThePlanHashSoOldStateIsRefused()
    {
        var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });

        async Task RunOnceAsync(IReadOnlyList<int>? fetchKeys)
        {
            var uploaded = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var checkpointed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectors = new ConnectorManager();
            connectors.AddSource(new FourColumnSourceFactory());
            connectors.AddSink(new ConfigurableGroupedSinkFactory([0], fetchKeys, uploaded));
            await using var stream = new FlowtideBuilder("sink-role-hash-red")
                .AddPlan(Write(Read("t", false), 4), false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(LateMaterializationMode.ForceOn)
                .SetVersionParts([(null, true)])
                .WithCheckpointListener(new CheckpointDoneListener(checkpointed))
                .WithFailureListener(e => { if (e != null) failed.TrySetResult(e); })
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();

            await stream.StartAsync();
            var finished = await Task.WhenAny(checkpointed.Task, failed.Task).WaitAsync(TimeSpan.FromSeconds(15));
            if (finished == failed.Task)
            {
                throw await failed.Task;
            }
            await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(15));
        }

        await RunOnceAsync(null);
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => RunOnceAsync([0]));
        Assert.Contains("Stream plan hash stored in storage is different", refused.Message);
    }

    [Fact]
    public async Task ReusingTheSamePlannedSinkDoesNotChangeItsHash()
    {
        var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        var plan = Write(Read("t", false), 4);
        string? expectedHash = null;

        async Task RunOnceAsync(bool firstRun)
        {
            var uploaded = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var checkpointed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectors = new ConnectorManager();
            connectors.AddSource(new FourColumnSourceFactory());
            connectors.AddSink(new ConfigurableGroupedSinkFactory([0], [0], uploaded));
            await using var stream = new FlowtideBuilder("same-sink-role-hash-red")
                .AddPlan(plan, false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(LateMaterializationMode.ForceOn)
                .SetVersionParts([(null, true)])
                .WithCheckpointListener(new CheckpointDoneListener(checkpointed))
                .WithFailureListener(e => { if (e != null) failed.TrySetResult(e); })
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();

            var context = (FlowtideDotNet.Base.Engine.Internal.StateMachine.StreamContext)typeof(FlowtideDotNet.Base.Engine.DataflowStream)
                .GetField("streamContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(stream)!;
            string hash = context._streamVersionInformation!.Hash;
            expectedHash ??= hash;
            Assert.Equal(expectedHash, hash);
            await stream.StartAsync();
            var finished = await Task.WhenAny(checkpointed.Task, failed.Task, Task.Delay(firstRun ? 15000 : 2000));
            if (finished == failed.Task)
            {
                throw await failed.Task;
            }
            Assert.True(!firstRun || checkpointed.Task.IsCompleted);
            await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(15));
        }

        await RunOnceAsync(true);
        await RunOnceAsync(false);
    }

    [Fact]
    public async Task MovingTheFetchRoleBetweenSameNamedWritesChangesTheHash()
    {
        async Task<string> BuildHash(string fetchingColumn)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var first = (WriteRelation)Write(Read("a", false), 4).Relations[0];
            var second = (WriteRelation)Write(Read("b", false), 4).Relations[0];
            first.TableSchema.Names[0] = "first";
            second.TableSchema.Names[0] = "second";
            var connectors = new ConnectorManager();
            connectors.AddSource(new FourColumnSourceFactory());
            connectors.AddSink(new OccurrenceGroupedSinkFactory(fetchingColumn));
            await using var stream = new FlowtideBuilder("sink-occurrence-hash-red")
                .AddPlan(new Plan { Relations = [first, second] }, false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(LateMaterializationMode.ForceOn)
                .SetVersionParts([(null, true)])
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
            var context = (FlowtideDotNet.Base.Engine.Internal.StateMachine.StreamContext)typeof(FlowtideDotNet.Base.Engine.DataflowStream)
                .GetField("streamContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(stream)!;
            return context._streamVersionInformation!.Hash;
        }

        Assert.NotEqual(await BuildHash("first"), await BuildHash("second"));
    }

    [Fact]
    public async Task MultiJoinPushedDownRowFunctionRunsWithoutConvertingRowReferences()
    {
        static Plan BuildRowFunctionMultiJoinPlan()
        {
            var first = new MergeJoinRelation
            {
                Left = Read("a", false),
                Right = Read("b", false),
                Type = JoinType.Inner,
                LeftKeys = [Field(0)],
                RightKeys = [Field(4)],
                Emit = [0, 1, 4]
            };
            var second = new MergeJoinRelation
            {
                Left = first,
                Right = Read("c", false),
                Type = JoinType.Inner,
                LeftKeys = [Field(2)],
                RightKeys = [Field(3)],
                PostJoinFilter = new ScalarFunction
                {
                    ExtensionUrn = "extension:test:rows",
                    ExtensionName = "is_odd",
                    Arguments = [Field(1)]
                }
            };
            var plan = Write(second, 7);
            Assert.Equal(1, MultiJoinMarker.Mark(plan, 8));
            return plan;
        }

        await using var pair = DifferentialPair.Planned(
            BuildRowFunctionMultiJoinPlan,
            () =>
            [
                Table("a", Row(1, 5, "a1", "n1")),
                Table("b", Row(1, 10, "b1", "n2")),
                Table("c", Row(1, 20, "c1", "n3"))
            ],
            RegisterRowsOnly);
        await pair.StartAsync();
        await pair.Expect("1|5|1|1|20|c1|n3");
    }

    [Fact]
    public void SourceKeyColumnMatchingIsCaseInsensitive()
    {
        var plan = Write(Read("t", false), 4);
        var connectors = new ConnectorManager();
        connectors.AddSource(new NamedKeySourceFactory(["K"]));
        var uploaded = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        connectors.AddSink(new ConfigurableGroupedSinkFactory([0], null, uploaded));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        int stores = LateMaterializationPass.Run(plan, connectors, functions, 1);

        Assert.Equal(1, stores);
    }
}
