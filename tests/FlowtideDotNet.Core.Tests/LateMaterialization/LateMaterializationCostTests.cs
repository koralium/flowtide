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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    private sealed class Sources : RegexConnectorSourceFactory
    {
        public Sources() : base("*")
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => readRelation.NamedTable.DotSeperated.StartsWith("plain") ? null : readRelation.NamedTable.DotSeperated.StartsWith("log") ? [] : ["k"];
    }

    private sealed class Sinks : RegexConnectorSinkFactory
    {
        private readonly IReadOnlyList<int>? _keys;

        public Sinks(IReadOnlyList<int>? keys) : base("*")
        {
            _keys = keys;
        }

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation) => _keys;

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private static ReadRelation Wide(string name, int payload, SubstraitBaseType? type = null)
    {
        var names = new List<string>() { "k" };
        var types = new List<SubstraitBaseType>() { new Int64Type() };
        for (int i = 0; i < payload; i++)
        {
            names.Add("c" + i);
            types.Add(type ?? new AnyType());
        }
        return new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = [name] },
            BaseSchema = new NamedStruct() { Names = names, Struct = new Struct() { Types = types } }
        };
    }

    private static Relation JoinedTo(Relation left, string table)
    {
        return new MergeJoinRelation()
        {
            Left = left,
            Right = Wide(table, 0),
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(left.OutputLength)]
        };
    }

    private static Relation FourJoins(Relation fact)
    {
        return JoinedTo(JoinedTo(JoinedTo(JoinedTo(fact, "d1"), "d2"), "d3"), "d4");
    }

    private static (LateMaterializationReport report, ConnectorManager connectors, FunctionsRegister functions) Analyze(Plan plan, int parallelism = 1, IReadOnlyList<int>? sinkKeys = null)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks(sinkKeys));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        var before = SubstraitSerializer.SerializeToJson(plan);
        var report = LateMaterializationPass.Analyze(plan, connectors, functions, parallelism);
        Assert.Equal(before, SubstraitSerializer.SerializeToJson(plan));
        return (report, connectors, functions);
    }

    private static RowStoreCandidate Of(LateMaterializationReport report, string table)
    {
        return Assert.Single(report.Hosts, x => x.Table == table);
    }

    [Fact]
    public void EightColumnsThroughFourStoredCopiesAreAdmitted()
    {
        var (report, _, _) = Analyze(Write(FourJoins(Wide("fact", 8)), 13));

        var fact = Of(report, "fact");
        Assert.True(fact.Admitted, fact.Reason);
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], fact.Columns);
        Assert.Equal(4, fact.Copies);
        Assert.Equal(8 * 16 * 4, fact.Saving);
        Assert.Equal(4 * 8 + (8 + 8 + 8) + 16, fact.Cost);
        Assert.Equal(1, report.Admitted);
    }

    [Fact]
    public void TheSameColumnsInsideOneMultiJoinGroupAreDeclined()
    {
        var plan = Write(FourJoins(Wide("fact", 8)), 13);
        Assert.Equal(1, MultiJoinMarker.Mark(plan, 8));
        var (report, _, _) = Analyze(plan);

        var fact = Of(report, "fact");
        Assert.False(fact.Admitted);
        Assert.Equal(1, fact.Copies);
        Assert.Equal(8 * 16, fact.Saving);
        Assert.Equal(8 + (8 + 8 + 8) + 16, fact.Cost);
        Assert.Contains("under 128", fact.Reason);
    }

    [Fact]
    public void OneIntegerColumnIsDeclined()
    {
        var (report, _, _) = Analyze(Write(FourJoins(Wide("fact", 1, new Int64Type())), 6));

        var fact = Of(report, "fact");
        Assert.False(fact.Admitted);
        Assert.Equal(4, fact.Copies);
        Assert.Equal(8 * 4, fact.Saving);
        Assert.True(fact.Net < 0);
    }

    [Fact]
    public void AStringCountsMoreThanANumberAndNoColumnBelowAReference()
    {
        var strings = Of(Analyze(Write(JoinedTo(Wide("fact", 2, new StringType()), "d1"), 4)).report, "fact");
        var flags = Of(Analyze(Write(JoinedTo(Wide("fact", 8, new BoolType()), "d1"), 10)).report, "fact");

        Assert.Equal(2 * 32, strings.Saving);
        Assert.Equal(8 * 8, flags.Saving);
    }

    [Fact]
    public void AColumnReadBeforeAnyStoredCopyIsNotStored()
    {
        var filter = new FilterRelation()
        {
            Input = Wide("fact", 8),
            Condition = Compare(FunctionsComparison.GreaterThan, Field(3), new NumericLiteral() { Value = 1 })
        };
        var (report, _, _) = Analyze(Write(FourJoins(filter), 13));

        var fact = Of(report, "fact");
        Assert.True(fact.Admitted, fact.Reason);
        Assert.Equal([1, 2, 4, 5, 6, 7, 8], fact.Columns);
    }

    [Fact]
    public void TheRestIsFetchedAfterAFilterThatReadsOneColumn()
    {
        var filter = new FilterRelation()
        {
            Input = JoinedTo(JoinedTo(Wide("fact", 8), "d1"), "d2"),
            Condition = Compare(FunctionsComparison.GreaterThan, Field(3), new NumericLiteral() { Value = 1 })
        };
        var plan = Write(JoinedTo(JoinedTo(filter, "d3"), "d4"), 13);
        var (report, connectors, functions) = Analyze(plan);

        var fact = Of(report, "fact");
        Assert.True(fact.Admitted, fact.Reason);
        Assert.Equal(8, fact.Columns.Count);
        Assert.Equal(2, fact.Fetches);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection()));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        var first = Assert.IsType<FetchRequiredColumnsRelation>(filter.Input);
        Assert.Equal([2], Assert.Single(first.Fetches).Columns);
        var last = Assert.IsType<FetchRequiredColumnsRelation>(plan.Relations.OfType<WriteRelation>().Single().Input);
        Assert.Equal(7, Assert.Single(last.Fetches).Columns.Count);
    }

    [Fact]
    public void ColumnsThatAreReadAndCarriedOnAreReportedForAStoreOfTheirOwn()
    {
        Expression condition = Compare(FunctionsComparison.GreaterThan, Field(1), new NumericLiteral() { Value = 1 });
        for (int i = 2; i <= 8; i++)
        {
            condition = new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.And,
                Arguments = [condition, Compare(FunctionsComparison.GreaterThan, Field(i), new NumericLiteral() { Value = 1 })]
            };
        }
        var filter = new FilterRelation() { Input = JoinedTo(JoinedTo(Wide("fact", 8), "d1"), "d2"), Condition = condition };
        var (report, _, _) = Analyze(Write(JoinedTo(JoinedTo(filter, "d3"), "d4"), 13));

        var fact = Of(report, "fact");
        Assert.True(fact.Admitted, fact.Reason);
        Assert.Equal(2, fact.Copies);
        var later = Assert.Single(report.MidPlan);
        Assert.Equal(nameof(FilterRelation), later.Producer);
        Assert.Equal(8, later.Columns);
        Assert.Equal(8 * 16 * 2, later.Saving);
    }

    [Fact]
    public void ASourceThatCannotHostIsReportedForAStoreOfItsOwn()
    {
        var (report, _, _) = Analyze(Write(FourJoins(Wide("plain", 8)), 13));

        Assert.DoesNotContain(report.Hosts, x => x.Table == "plain");
        Assert.Contains(report.MidPlan, x => x.Producer == nameof(ReadRelation) && x.Saving == 8 * 16 * 4);
        Assert.Equal(0, report.Admitted);
    }

    [Fact]
    public void PartitionsOfOneJoinCountAsTheOneCopyTheyAre()
    {
        Plan One() => Write(JoinedTo(Wide("fact", 1, new StringType()), "d1"), 3);
        var plain = Of(Analyze(One()).report, "fact");
        var partitioned = Of(Analyze(Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(One(), 8)).report, "fact");

        Assert.Equal(1, plain.Copies);
        Assert.Equal(1, partitioned.Copies, 6);
        Assert.Equal(plain.Saving, partitioned.Saving);
        Assert.False(plain.Admitted);
        Assert.False(partitioned.Admitted);
    }

    [Fact]
    public void WidePartitionedJoinsAreAdmittedAsTheUnpartitionedAre()
    {
        var plain = Of(Analyze(Write(FourJoins(Wide("fact", 8)), 13)).report, "fact");
        var partitioned = Of(Analyze(Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(Write(FourJoins(Wide("fact", 8)), 13), 3)).report, "fact");

        Assert.True(partitioned.Admitted, partitioned.Reason);
        Assert.Equal(plain.Copies, partitioned.Copies, 6);
        Assert.Equal(plain.Saving, partitioned.Saving);
    }

    [Fact]
    public void ANarrowColumnReadAfterOneCopyIsNotWorthItsFetch()
    {
        var fact = Wide("fact", 8, new StringType());
        fact.BaseSchema.Names.Insert(1, "j");
        fact.BaseSchema.Struct!.Types.Insert(1, new Int64Type());
        var second = new MergeJoinRelation()
        {
            Left = JoinedTo(fact, "d1"),
            Right = Wide("d2", 0),
            Type = JoinType.Inner,
            LeftKeys = [Field(1)],
            RightKeys = [Field(11)]
        };
        var (report, _, _) = Analyze(Write(second, 12));

        var decided = Of(report, "fact");
        Assert.True(decided.Admitted, decided.Reason);
        Assert.Equal([2, 3, 4, 5, 6, 7, 8, 9], decided.Columns);
        Assert.Equal(1, decided.Fetches);
    }

    [Fact]
    public void AKeyedSinkIsOneMoreCopyAndASourceWithoutKeysPaysForItsHeap()
    {
        var plain = Of(Analyze(Write(JoinedTo(Wide("fact", 8), "d1"), 10)).report, "fact");
        var keyed = Of(Analyze(Write(JoinedTo(Wide("fact", 8), "d1"), 10), sinkKeys: [0]).report, "fact");
        var log = Of(Analyze(Write(JoinedTo(Wide("log", 8), "d1"), 10), sinkKeys: [0]).report, "log");

        Assert.Equal(1, plain.Copies);
        Assert.Equal(2, keyed.Copies);
        Assert.Equal(2 * 8 + (8 + 8 + 8) + 16, keyed.Cost);
        Assert.Equal(8, log.Columns.Count);
        Assert.Equal(2 * 8 + 8 * 16 + 16, log.Cost);
    }

    [Fact]
    public void APlainJoinUnderBuilderParallelismAndARowConditionInAGroupSaveNothing()
    {
        var parallel = Of(Analyze(Write(FourJoins(Wide("fact", 8)), 13), parallelism: 2).report, "fact");
        Assert.Empty(parallel.Columns);

        var plan = Write(FourJoins(Wide("fact", 8)), 13);
        var top = (MergeJoinRelation)plan.Relations.OfType<WriteRelation>().Single().Input;
        top.PostJoinFilter = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "is_odd", Arguments = [Field(0)] };
        Assert.Equal(1, MultiJoinMarker.Mark(plan, 8));
        var grouped = Of(Analyze(plan).report, "fact");
        Assert.Empty(grouped.Columns);
        Assert.False(grouped.Admitted);
    }

    private sealed class OtherStruct : StructExpression
    {
    }

    [Fact]
    public void AnExpressionThatCannotBeCopiedKeepsItsInputPlain()
    {
        var filter = new FilterRelation()
        {
            Input = Wide("fact", 8),
            Condition = Compare(FunctionsComparison.Equal, Field(1), new OtherStruct() { Fields = [Field(2)] })
        };
        var plan = Write(FourJoins(filter), 13);
        var (report, connectors, functions) = Analyze(plan);

        Assert.Empty(Of(report, "fact").Columns);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.Equal(9, Assert.IsType<FetchRequiredColumnsRelation>(filter.Input).OutputLength);
    }

    [Fact]
    public void ASelectionOfSomeColumnsStoresOnlyThose()
    {
        var plan = Write(FourJoins(Wide("fact", 8)), 13);
        var (report, connectors, functions) = Analyze(plan);
        var selection = report.Selection();
        var host = Assert.Single(selection).Key;
        selection[host] = [2, 5];
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1, selection));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        var read = Assert.IsType<ReadRelation>(host);
        Assert.Equal([2, 5], read.RowStore!.StoredColumns);
        Assert.Equal(8, read.OutputLength);
    }

    [Theory]
    [InlineData(10000, 5, 6, 16)]
    [InlineData(1000, 5, 6, 6)]
    [InlineData(400, 5, 6, 0)]
    [InlineData(128, 1, 1, 3)]
    public void TheCacheSizesAStoresWriteQueue(int cachePages, int stores, int readers, int pages)
    {
        Assert.Equal(pages, Math.Max(0, FlowtideBuilder.RowStoreWriteQueuePages(cachePages, stores, readers)));
    }

    [Fact]
    public void AnAdmittedSelectionStoresOnlyItsColumns()
    {
        var plan = Write(FourJoins(Wide("fact", 8)), 13);
        var (report, connectors, functions) = Analyze(plan);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection()));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        var stores = RowStoreHostCollector.Collect(plan.Relations);
        var host = Assert.IsType<ReadRelation>(Assert.Single(stores).Value);
        Assert.Equal("fact", host.NamedTable.DotSeperated);
        Assert.Equal(8, host.RowStore!.StoredColumns.Count);
    }

    private static Plan Chain()
    {
        Relation joined = Read("a", false);
        foreach (var table in new[] { "b", "c", "d", "e", "f" })
        {
            joined = new MergeJoinRelation()
            {
                Left = joined,
                Right = Read(table, false),
                Type = JoinType.Left,
                LeftKeys = [Field(0)],
                RightKeys = [Field(joined.OutputLength + 1)]
            };
        }
        return Write(joined, 24);
    }

    private static DifferentialTable[] ChainTables()
    {
        return
        [
            Table("a", Row(1, 10, "a1", "x"), Row(2, 20, "a2", null), Row(3, 30, null, "z")),
            Table("b", Row(10, 1, "b1", "p"), Row(11, 2, "b2", "q")),
            Table("c", Row(20, 1, "c1", null), Row(21, 3, "c3", "r")),
            Table("d", Row(30, 2, "d2", "s")),
            Table("e", Row(40, 1, "e1", "t"), Row(41, 1, "e1b", null)),
            Table("f", Row(50, 1, "f1", "u"), Row(51, 2, "f2", null))
        ];
    }

    [Fact]
    public async Task AutoPlacesTheAdmittedStoreAndGivesTheSameRows()
    {
        FlowtideBuilder? planned = null;
        await using var pair = DifferentialPair.Planned(Chain, ChainTables, builder => planned ??= builder, null, LateMaterializationMode.Auto);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());

        var report = planned!.LateMaterializationReport!;
        Assert.True(Of(report, "a").Admitted, report.ToString());
        Assert.Equal(5 * 3 * 16, Of(report, "a").Saving);
        Assert.Equal(5 * 8 + (8 + 8 + 8) + 16, Of(report, "a").Cost);
        Assert.False(Of(report, "b").Admitted);
        Assert.False(Of(report, "f").Admitted);
        Assert.Equal(1, Count(pair.Hosted, "Fetch required columns"));
        Assert.Equal(0, Count(pair.Plain, "Fetch required columns"));

        pair.Change(tables =>
        {
            tables["a"].Upsert(Row(2, 21, "a2b", "y"), Row(4, 40, "a4", "w"));
            tables["a"].Delete(3);
            tables["b"].Upsert(Row(12, 4, "b4", null));
        });
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());

        await pair.RestartAsync();
        pair.Change(tables => tables["e"].Upsert(Row(42, 2, "e2", "u")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }

    private static async Task<(string hash, int fetches, LateMaterializationReport? report, string plan)> Built(LateMaterializationMode mode, Func<FlowtideDotNet.Storage.Persistence.IPersistentStorage, FlowtideDotNet.Storage.StateManager.StateManagerOptions>? stateOptions = null)
    {
        FlowtideBuilder? used = null;
        Plan? built = null;
        await using var stream = new DifferentialStream(() => built = Chain(), builder => used = builder.SetLateMaterialization(mode), ChainTables()) { StateOptions = stateOptions };
        await stream.StartAsync();
        var context = typeof(Base.Engine.DataflowStream).GetField("streamContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(stream.Stream)!;
        var hash = ((Base.Engine.Internal.StateMachine.StreamContext)context)._streamVersionInformation!.Hash;
        return (hash, stream.Stream.GetDiagnosticsGraph().Nodes.Values.Count(x => x.DisplayName == "Fetch required columns"), used!.LateMaterializationReport, SubstraitSerializer.SerializeToJson(built!));
    }

    [Fact]
    public async Task PlaceboDecidesAsAutoAndLeavesThePlanAndItsHash()
    {
        var off = await Built(LateMaterializationMode.Off);
        var placebo = await Built(LateMaterializationMode.Placebo);
        var auto = await Built(LateMaterializationMode.Auto);

        Assert.Null(off.report);
        Assert.Equal(SubstraitSerializer.SerializeToJson(Chain()), off.plan);
        Assert.Equal(off.plan, placebo.plan);
        Assert.NotEqual(off.plan, auto.plan);
        Assert.Equal(off.hash, placebo.hash);
        Assert.Equal(0, placebo.fetches);
        Assert.Equal(1, placebo.report!.Admitted);
        Assert.Equal(1, auto.report!.Admitted);
        Assert.NotEqual(off.hash, auto.hash);
        Assert.Equal(1, auto.fetches);
    }

    [Fact]
    public void ForceOnOnACacheTooSmallFailsTheBuildWithTheSizeItNeeds()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = RunnableBuilder(Write(FourJoins(Wide("fact", 8)), 13), LateMaterializationMode.ForceOn)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage, CachePageCount = 40, MinCachePageCount = 40 });

        var refused = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("a cache of 40 pages cannot hold their pages", refused.Message);
        Assert.Contains("Set CachePageCount to", refused.Message);
    }

    private sealed class SourcesThatCannotSay : RegexConnectorSourceFactory
    {
        public SourcesThatCannotSay() : base("*")
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new DifferentialSource(readRelation, new DifferentialTable(readRelation.NamedTable.DotSeperated, readRelation.BaseSchema.Names.Count), functionsRegister);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => throw new InvalidOperationException("the keys are not known");
    }

    [Fact]
    public async Task APlanTheAnalysisCannotReadIsBuiltAsOffBuildsIt()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var plan = Write(FourJoins(Wide("fact", 8)), 13);
        var before = SubstraitSerializer.SerializeToJson(plan);
        var connectors = new ConnectorManager();
        connectors.AddSource(new SourcesThatCannotSay());
        connectors.AddSink(new RunnableSinks());
        var builder = new FlowtideBuilder("cannot-read-" + Guid.NewGuid().ToString("N"))
            .AddPlan(plan, false)
            .AddConnectorManager(connectors)
            .SetLateMaterialization(LateMaterializationMode.Auto)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });

        await using var stream = builder.Build();

        Assert.Contains("could not be analysed: the keys are not known", builder.LateMaterializationReport!.Skipped);
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
        Assert.Equal(before, SubstraitSerializer.SerializeToJson(plan));
    }

    [Fact]
    public void TheCostOfAStoreIsItsReferencesItsIdentityAndItsFetches()
    {
        var (report, _, _) = Analyze(Write(JoinedTo(Wide("fact", 8), "d1"), 10));
        var one = Of(report, "fact");

        Assert.Equal(8 * 16, one.Saving);
        Assert.Equal(8 + (8 + 8 + 8) + 16, one.Cost);
        Assert.Equal(1, one.Fetches);
    }

    [Fact]
    public async Task TwoBuildsOfOnePlanAdmitTheSameStoresAndGetTheSameHash()
    {
        var first = await Built(LateMaterializationMode.Auto);
        var second = await Built(LateMaterializationMode.Auto);

        Assert.Equal(first.plan, second.plan);
        Assert.Equal(first.hash, second.hash);
        Assert.Equal(first.report!.ToString(), second.report!.ToString());
    }

    [Fact]
    public async Task AutoLeavesAStreamAloneWhoseCacheCannotHoldItsStores()
    {
        var off = await Built(LateMaterializationMode.Off);
        var small = await Built(LateMaterializationMode.Auto, storage => new FlowtideDotNet.Storage.StateManager.StateManagerOptions() { PersistentStorage = storage, CachePageCount = 60, MinCachePageCount = 60 });

        Assert.Contains("cannot hold the pages of 1 row stores", small.report!.Skipped);
        Assert.Equal(off.plan, small.plan);
        Assert.Equal(off.hash, small.hash);
        Assert.Equal(0, small.fetches);
    }

    [Fact]
    public async Task AutoLeavesAStreamAloneWhoseStorageCannotHoldRowStores()
    {
        var directory = Path.Join(Path.GetTempPath(), "lm-filecache-" + Guid.NewGuid().ToString("N"));
        try
        {
            var off = await Built(LateMaterializationMode.Off);
            var other = await Built(LateMaterializationMode.Auto, _ => new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
            {
                PersistentStorage = new FlowtideDotNet.Storage.Persistence.CacheStorage.FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions() { DirectoryPath = directory })
            });

            Assert.Contains("Reservoir", other.report!.Skipped);
            Assert.Equal(off.plan, other.plan);
            Assert.Equal(off.hash, other.hash);
            Assert.Equal(0, other.fetches);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
        }
    }
}
