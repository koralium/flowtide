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
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBPr10bRedTests
{
    private sealed class KeyedSourceFactory(bool hostable = true) : RegexConnectorSourceFactory("*")
    {
        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new WideSingleRowSource(readRelation, functionsRegister, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => hostable ? ["k"] : null;
    }

    private sealed class WideSingleRowSource : ColumnBatchReadBaseOperator
    {
        private readonly int _columns;

        public WideSingleRowSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options)
            : base(readRelation, functionsRegister, options)
        {
            _columns = readRelation.BaseSchema.Names.Count;
        }

        public override string DisplayName => "wide single row source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "w" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            var allocator = GlobalMemoryManager.Instance;
            var cols = new IColumn[_columns];
            for (int c = 0; c < _columns; c++)
            {
                cols[c] = Column.Create(allocator);
            }
            for (int row = 1; row <= 256; row++)
            {
                cols[0].Add(new Int64Value(row));
                for (int c = 1; c < _columns; c++)
                {
                    cols[c].Add(new StringValue($"v{row}_{c}"));
                }
            }
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            for (int row = 0; row < 256; row++)
            {
                weights.Add(1);
                iterations.Add(0);
            }
            yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(cols)), 1);
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            await EnterCheckpointLock();
            try
            {
                var allocator = GlobalMemoryManager.Instance;
                var cols = new IColumn[_columns];
                for (int c = 0; c < _columns; c++)
                {
                    cols[c] = Column.Create(allocator);
                }
                yield return new DeltaReadEvent(
                    new EventBatchWeighted(new PrimitiveList<int>(allocator), new PrimitiveList<uint>(allocator), new EventBatchData(cols)),
                    new Watermark("w", LongWatermarkValue.Create(1)));
            }
            finally
            {
                ExitCheckpointLock();
            }
        }
    }

    private sealed class GroupedCollectSink : ColumnGroupedWriteOperator
    {
        private readonly IReadOnlyList<int> _keys;
        private readonly TaskCompletionSource<List<string>> _done;

        public GroupedCollectSink(WriteRelation writeRelation, IReadOnlyList<int> keys, TaskCompletionSource<List<string>> done, ExecutionDataflowBlockOptions options)
            : base(ExecutionMode.OnCheckpoint, writeRelation, options)
        {
            _keys = keys;
            _done = done;
        }

        public override string DisplayName => "grouped collect sink";
        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(_keys);
        protected override void Checkpoint(long checkpointTime) { }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            var uploaded = new List<string>();
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                if (!row.IsDeleted)
                {
                    uploaded.Add(row.EventBatchData.Columns[0].GetValueAt(row.Index, default).ToString()!);
                }
            }
            if (uploaded.Count > 0)
            {
                _done.TrySetResult(uploaded);
            }
        }
    }

    private sealed class CollectSinkFactory(IReadOnlyList<int> keys, TaskCompletionSource<List<string>> done) : RegexConnectorSinkFactory("*")
    {
        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => new GroupedCollectSink(writeRelation, keys, done, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private sealed class LegacyFactory(IReadOnlyList<int> sinkKeys, TaskCompletionSource<List<string>> done) : IReadWriteFactory
    {
        public ReadOperatorInfo GetReadOperator(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new(new WideSingleRowSource(readRelation, functionsRegister, dataflowBlockOptions));

        public IStreamEgressVertex GetWriteOperator(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
            => new GroupedCollectSink(writeRelation, sinkKeys, done, executionDataflowBlockOptions);
    }

    private sealed class DerivedStructExpression : StructExpression
    {
    }

    private static ReadRelation SchemaRead(string name, params (string Name, SubstraitBaseType Type)[] columns)
    {
        return new ReadRelation
        {
            NamedTable = new NamedTable { Names = [name] },
            BaseSchema = new NamedStruct
            {
                Names = columns.Select(x => x.Name).ToList(),
                Struct = new Struct { Types = columns.Select(x => x.Type).ToList() }
            }
        };
    }

    private static Plan WideNormalizedFourJoins()
    {
        var cols = new List<(string, SubstraitBaseType)> { ("k", new Int64Type()) };
        for (int i = 0; i < 8; i++)
        {
            cols.Add(("c" + i, new StringType()));
        }
        Relation current = new NormalizationRelation
        {
            Input = SchemaRead("fact", cols.ToArray()),
            KeyIndex = [0]
        };
        for (int d = 1; d <= 4; d++)
        {
            current = new MergeJoinRelation
            {
                Left = current,
                Right = SchemaRead("d" + d, ("k", new Int64Type())),
                Type = JoinType.Inner,
                LeftKeys = [Field(0)],
                RightKeys = [Field(current.OutputLength)]
            };
        }
        return Write(current, 13);
    }

    [Fact]
    public async Task AutoDeclinesWhenPersistentStorageIsNotReservoir()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "lm-nonreservoir-" + Guid.NewGuid().ToString("N"));
        try
        {
            var done = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectors = new ConnectorManager();
            connectors.AddSource(new KeyedSourceFactory(false));
            connectors.AddSink(new CollectSinkFactory([0], done));

            await using var stream = new FlowtideBuilder("auto-default-storage-red")
                .AddPlan(WideNormalizedFourJoins(), false)
                .AddConnectorManager(connectors)
                .WithFailureListener(ex => { if (ex != null) failed.TrySetResult(ex); })
                .WithStateOptions(new StateManagerOptions
                {
                    TemporaryStorageOptions = new FlowtideDotNet.Storage.FileCacheOptions { DirectoryPath = tempDir }
                })
                .Build();

            await stream.StartAsync();
            var finished = await Task.WhenAny(done.Task, failed.Task).WaitAsync(TimeSpan.FromSeconds(15));
            if (finished == failed.Task)
            {
                throw await failed.Task;
            }
            Assert.Equal(256, (await done.Task).Count);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Fact]
    public async Task AutoDeclinesWhenReservoirProviderLocksOnReads()
    {
        var cacheDir = Path.Combine(Path.GetTempPath(), "lm-localcache-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions
            {
                FileProvider = new MemoryFileProvider(),
                CacheProvider = new LocalDiskProvider(cacheDir),
                MaxCacheSizeBytes = 1024 * 1024
            });
            Assert.False(storage.ReadsPagesWithoutFileLock);

            var done = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectors = new ConnectorManager();
            connectors.AddSource(new KeyedSourceFactory(false));
            connectors.AddSink(new CollectSinkFactory([0], done));
            var builder = new FlowtideBuilder("auto-unqualified-provider-red")
                .AddPlan(WideNormalizedFourJoins(), false)
                .AddConnectorManager(connectors)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage });

            await using var stream = builder.Build();
            Assert.Equal(0, builder.LateMaterializationReport?.Admitted ?? 0);
        }
        finally
        {
            if (Directory.Exists(cacheDir))
            {
                Directory.Delete(cacheDir, true);
            }
        }
    }

    [Fact]
    public void ScatterPartitionedSingleJoinDoesNotMultiplyStoredCopiesByPartitionCount()
    {
        var orders = SchemaRead("orders", ("k", new Int64Type()), ("userkey", new Int64Type()));
        var users = SchemaRead(
            "users",
            ("k", new Int64Type()),
            ("firstName", new StringType()),
            ("lastName", new StringType()),
            ("companyId", new Int64Type()));
        var join = new MergeJoinRelation
        {
            Left = orders,
            Right = users,
            Type = JoinType.Inner,
            LeftKeys = [Field(1)],
            RightKeys = [Field(2)]
        };
        var plan = ParallelizeOptimizer.Optimize(Write(join, 6), 4);
        var connectors = new ConnectorManager();
        connectors.AddSource(new KeyedSourceFactory(true));
        connectors.AddSink(new CollectSinkFactory([0], new TaskCompletionSource<List<string>>()));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        var candidate = Assert.Single(report.Hosts, x => x.Table == "users");
        Assert.Equal(1, candidate.Copies);
        Assert.False(candidate.Admitted);
    }

    [Fact]
    public async Task AutoCacheBudgetAccountsForParallelPartitionFetchReaders()
    {
        static Plan BuildParallelTailPlan()
        {
            var cols = new List<(string, SubstraitBaseType)> { ("k", new Int64Type()) };
            for (int i = 0; i < 8; i++)
            {
                cols.Add(("c" + i, new StringType()));
            }
            Relation chain = SchemaRead("fact", cols.ToArray());
            for (int d = 1; d <= 3; d++)
            {
                chain = new MergeJoinRelation
                {
                    Left = chain,
                    Right = SchemaRead("d" + d, ("k", new Int64Type())),
                    Type = JoinType.Inner,
                    LeftKeys = [Field(0)],
                    RightKeys = [Field(chain.OutputLength)]
                };
            }
            var finalJoin = new MergeJoinRelation
            {
                Left = chain,
                Right = SchemaRead("d4", ("k", new Int64Type())),
                Type = JoinType.Inner,
                LeftKeys = [Field(0)],
                RightKeys = [Field(chain.OutputLength)]
            };
            var plan = ParallelizeOptimizer.Optimize(Write(finalJoin, 13), 19);
            var write = plan.Relations.OfType<WriteRelation>().Single();
            write.Input = new FilterRelation
            {
                Input = write.Input,
                Condition = Compare(FunctionsComparison.Equal, Field(1), Field(1))
            };
            return plan;
        }

        static async Task<int> RunWithModeAsync(LateMaterializationMode mode)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var done = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectors = new ConnectorManager();
            connectors.AddSource(new KeyedSourceFactory(true));
            connectors.AddSink(new CollectSinkFactory([0], done));

            await using var stream = new FlowtideBuilder("auto-reader-budget-" + mode)
                .AddPlan(BuildParallelTailPlan(), false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(mode)
                .WithFailureListener(ex => { if (ex != null) failed.TrySetResult(ex); })
                .WithStateOptions(new StateManagerOptions
                {
                    CachePageCount = 720,
                    MinCachePageCount = 100,
                    PersistentStorage = storage
                })
                .Build();

            await stream.StartAsync();
            var finished = await Task.WhenAny(done.Task, failed.Task).WaitAsync(TimeSpan.FromSeconds(15));
            if (finished == failed.Task)
            {
                throw await failed.Task;
            }
            return (await done.Task).Count;
        }

        Assert.Equal(256, await RunWithModeAsync(LateMaterializationMode.Off));
        Assert.Equal(256, await RunWithModeAsync(LateMaterializationMode.Auto));
    }

    [Fact]
    public async Task AutoDoesNotThrowWhenUncopyableExpressionIsNestedInsideScalarFunction()
    {
        Plan PlanWithNestedDerivedStruct()
        {
            var plan = WideNormalizedFourJoins();
            var write = (WriteRelation)plan.Relations[0];
            write.Input = new FilterRelation
            {
                Input = write.Input,
                Condition = new ScalarFunction
                {
                    ExtensionUrn = "extension:test:fn",
                    ExtensionName = "accept_any",
                    Arguments = [new DerivedStructExpression { Fields = [Field(0)] }]
                }
            };
            return plan;
        }

        static FlowtideDotNet.Base.Engine.DataflowStream BuildWithMode(Plan plan, LateMaterializationMode mode, ReservoirPersistentStorage storage)
        {
            var connectors = new ConnectorManager();
            connectors.AddSource(new KeyedSourceFactory(false));
            connectors.AddSink(new CollectSinkFactory([0], new TaskCompletionSource<List<string>>()));
            var builder = new FlowtideBuilder("nested-expr-" + mode)
                .AddPlan(plan, false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(mode)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage });
            builder.FunctionsRegister.RegisterColumnScalarFunction(
                "extension:test:fn",
                "accept_any",
                (_, _, _, _) => System.Linq.Expressions.Expression.Constant(true));
            return builder.Build();
        }

        using var offStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        await using var off = BuildWithMode(PlanWithNestedDerivedStruct(), LateMaterializationMode.Off, offStorage);

        using var autoStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        await using var auto = BuildWithMode(PlanWithNestedDerivedStruct(), LateMaterializationMode.Auto, autoStorage);
    }

    [Fact]
    public async Task AutoWithLegacyReadWriteFactoryDoesNotFuseFetchedSinkPrimaryKey()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        var done = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = new LegacyFactory([1], done);

#pragma warning disable CS0618
        await using var stream = new FlowtideBuilder("legacy-rwf-auto-red")
            .AddPlan(WideNormalizedFourJoins(), false)
            .AddReadWriteFactory(factory)
            .WithFailureListener(ex => { if (ex != null) failed.TrySetResult(ex); })
            .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
            .Build();
#pragma warning restore CS0618

        await stream.StartAsync();
        var finished = await Task.WhenAny(done.Task, failed.Task).WaitAsync(TimeSpan.FromSeconds(15));
        if (finished == failed.Task)
        {
            throw await failed.Task;
        }
        Assert.Equal(256, (await done.Task).Count);
    }

    [Fact]
    public void UnionOriginsDoesNotAdmitStoreMaterializedBeforeAllJoinsByKeySlotOrder()
    {
        var cols = new List<(string, SubstraitBaseType)> { ("k0", new Int64Type()) };
        for (int i = 1; i <= 8; i++)
        {
            cols.Add(("c" + i, new StringType()));
        }
        cols.Add(("k9", new Int64Type()));

        var host = new NormalizationRelation
        {
            Input = SchemaRead("fact", cols.ToArray()),
            KeyIndex = [0, 9]
        };
        var branch0 = new FilterRelation
        {
            Input = new ReferenceRelation { RelationId = 0 },
            Condition = Compare(FunctionsComparison.Equal, Field(0), Field(0)),
            Emit = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        };
        var branch1 = new ReferenceRelation { RelationId = 0 };
        Relation current = new SetRelation
        {
            Operation = SetOperation.UnionAll,
            Inputs = [branch0, branch1]
        };
        for (int d = 1; d <= 4; d++)
        {
            current = new MergeJoinRelation
            {
                Left = current,
                Right = SchemaRead("d" + d, ("k0", new Int64Type())),
                Type = JoinType.Inner,
                LeftKeys = [Field(0)],
                RightKeys = [Field(current.OutputLength)]
            };
        }
        var plan = new Plan
        {
            Relations =
            [
                host,
                new WriteRelation
                {
                    Input = current,
                    NamedObject = new NamedTable { Names = ["output"] },
                    TableSchema = new NamedStruct { Names = Enumerable.Range(0, 14).Select(x => "c" + x).ToList() }
                }
            ]
        };
        var connectors = new ConnectorManager();
        connectors.AddSource(new KeyedSourceFactory(false));
        connectors.AddSink(new CollectSinkFactory([0], new TaskCompletionSource<List<string>>()));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        if (report.Admitted > 0)
        {
            LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());
            var write = (WriteRelation)plan.Relations[1];
            Assert.IsType<FetchRequiredColumnsRelation>(write.Input);
        }
    }

    [Fact]
    public async Task AutoDeclinesWhenPlanCannotBeSerializedToJson()
    {
        Plan PlanWithUnserializableArrayLiteral()
        {
            var plan = WideNormalizedFourJoins();
            var write = (WriteRelation)plan.Relations[0];
            write.Input = new FilterRelation
            {
                Input = write.Input,
                Condition = new ScalarFunction
                {
                    ExtensionUrn = "extension:test:fn",
                    ExtensionName = "accept_any",
                    Arguments = [new ArrayLiteral { Expressions = [Field(0)] }]
                }
            };
            return plan;
        }

        static FlowtideDotNet.Base.Engine.DataflowStream BuildWithMode(Plan plan, LateMaterializationMode mode, ReservoirPersistentStorage storage)
        {
            var connectors = new ConnectorManager();
            connectors.AddSource(new KeyedSourceFactory(false));
            connectors.AddSink(new CollectSinkFactory([0], new TaskCompletionSource<List<string>>()));
            var builder = new FlowtideBuilder("unserializable-plan-" + mode)
                .AddPlan(plan, false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(mode)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage });
            builder.FunctionsRegister.RegisterColumnScalarFunction(
                "extension:test:fn",
                "accept_any",
                (_, _, _, _) => System.Linq.Expressions.Expression.Constant(true));
            return builder.Build();
        }

        using var offStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        await using var off = BuildWithMode(PlanWithUnserializableArrayLiteral(), LateMaterializationMode.Off, offStorage);

        using var autoStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        await using var auto = BuildWithMode(PlanWithUnserializableArrayLiteral(), LateMaterializationMode.Auto, autoStorage);
    }

    [Fact]
    public async Task PlaceboAndAutoDoNotThrowWhenExchangeEmitWidensOutput()
    {
        static Plan PlanWithWideningExchangeEmit()
        {
            var read = SchemaRead("fact", ("k", new Int64Type()), ("v", new StringType()));
            var exchange = new ExchangeRelation
            {
                Input = read,
                PartitionCount = 1,
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget { PartitionIds = [0] }],
                Emit = [0, 1, 0]
            };
            var filter = new FilterRelation
            {
                Input = new StandardOutputExchangeReferenceRelation
                {
                    RelationId = 0,
                    TargetId = 0,
                    ReferenceOutputLength = 3
                },
                Condition = Compare(FunctionsComparison.Equal, Field(0), Field(0)),
                Emit = [2]
            };
            return new Plan
            {
                Relations =
                [
                    exchange,
                    new WriteRelation
                    {
                        Input = filter,
                        NamedObject = new NamedTable { Names = ["output"] },
                        TableSchema = new NamedStruct { Names = ["c0"] }
                    }
                ]
            };
        }

        static FlowtideDotNet.Base.Engine.DataflowStream BuildWithMode(LateMaterializationMode mode, ReservoirPersistentStorage storage)
        {
            var connectors = new ConnectorManager();
            connectors.AddSource(new KeyedSourceFactory(false));
            connectors.AddSink(new CollectSinkFactory([0], new TaskCompletionSource<List<string>>()));
            return new FlowtideBuilder("widening-exchange-" + mode)
                .AddPlan(PlanWithWideningExchangeEmit(), false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(mode)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
        }

        using var offStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        await using var off = BuildWithMode(LateMaterializationMode.Off, offStorage);

        using var placeboStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        await using var placebo = BuildWithMode(LateMaterializationMode.Placebo, placeboStorage);
    }

    [Fact]
    public void BranchedEightByteColumnsDoNotUnderchargeReferenceCopies()
    {
        var cols = new List<(string, SubstraitBaseType)> { ("k", new Int64Type()) };
        for (int i = 1; i <= 18; i++)
        {
            cols.Add(("c" + i, new Int64Type()));
        }
        var host = SchemaRead("fact", cols.ToArray());
        var branches = new List<Relation>();
        for (int b = 1; b <= 18; b++)
        {
            var projected = new ProjectRelation
            {
                Input = new ReferenceRelation { RelationId = 0, ReferenceOutputLength = 19 },
                Expressions = [],
                Emit = [0, b]
            };
            var joined = new MergeJoinRelation
            {
                Left = projected,
                Right = SchemaRead("d" + b, ("k", new Int64Type())),
                Type = JoinType.Inner,
                LeftKeys = [Field(0)],
                RightKeys = [Field(2)],
                Emit = [0]
            };
            branches.Add(joined);
        }
        var plan = new Plan
        {
            Relations =
            [
                host,
                new WriteRelation
                {
                    Input = new SetRelation { Operation = SetOperation.UnionAll, Inputs = branches },
                    NamedObject = new NamedTable { Names = ["output"] },
                    TableSchema = new NamedStruct { Names = ["k"] }
                }
            ]
        };
        var connectors = new ConnectorManager();
        connectors.AddSource(new KeyedSourceFactory(true));
        connectors.AddSink(new CollectSinkFactory([0], new TaskCompletionSource<List<string>>()));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        var candidate = Assert.Single(report.Hosts, x => x.Table == "fact");
        Assert.False(candidate.Admitted);
    }
}
