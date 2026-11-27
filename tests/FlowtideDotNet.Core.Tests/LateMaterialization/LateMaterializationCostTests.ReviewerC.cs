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
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
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

    [Fact]
    public void ReviewerCKeyedIdentityHashIsChargedBeforeAdmission()
    {
        var (report, _, _) = Analyze(Write(JoinedTo(Wide("fact", 5, new StringType()), "d1"), 7));

        var fact = Of(report, "fact");
        Assert.True(fact.Cost >= 40, $"The keyed reference and hash, join reference, and fetch cost at least 40, got {fact.Cost}.");
        Assert.False(fact.Admitted);
    }

    [Fact]
    public void ReviewerCInlineAliasesChargeTheAdditionalHeapCopy()
    {
        var read = Wide("fact", 5, new StringType());
        read.Emit = [0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5];
        Expression? condition = null;
        foreach (int position in new[] { 1, 3, 5, 7, 9 })
        {
            var comparison = Compare(FunctionsComparison.GreaterThan, Field(position), new StringLiteral() { Value = "a" });
            condition = condition == null ? comparison : new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.And,
                Arguments = [condition, comparison]
            };
        }
        var filter = new FilterRelation() { Input = read, Condition = condition! };
        var plan = Write(JoinedTo(filter, "d1"), 12);
        var (report, connectors, functions) = Analyze(plan);
        var fact = Of(report, "fact");
        Assert.Equal([2, 4, 6, 8, 10], fact.Columns);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1,
            new Dictionary<Relation, List<int>>() { [read] = fact.Columns }));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        var layout = RowStoreHost.ReadLayout(read.RowStore, 6, [0], read.Emit)!;
        Assert.Equal(layout.KeptColumns, layout.DeferredColumns);
        Assert.Equal(208, fact.Cost);
        Assert.False(fact.Admitted, $"The heap adds every retained payload column, but the model admits net {fact.Net}.");
    }

    private sealed class RunnableSources : RegexConnectorSourceFactory
    {
        public RunnableSources() : base("*") { }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new DifferentialSource(readRelation, new DifferentialTable(readRelation.NamedTable.DotSeperated, readRelation.BaseSchema.Names.Count), functionsRegister);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation) => ["k"];
    }

    private sealed class RunnableSinks : RegexConnectorSinkFactory
    {
        public RunnableSinks() : base("*") { }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => new DifferentialSink();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private static FlowtideBuilder RunnableBuilder(Plan plan, LateMaterializationMode mode)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new RunnableSources());
        connectors.AddSink(new RunnableSinks());
        return new FlowtideBuilder("review-c-" + Guid.NewGuid().ToString("N"))
            .AddPlan(plan, false).AddConnectorManager(connectors).SetLateMaterialization(mode);
    }

    [Fact]
    public async Task ReviewerCAutoDeclinesTheDefaultUnsupportedStorage()
    {
        var builder = RunnableBuilder(Write(FourJoins(new NormalizationRelation()
        {
            Input = Wide("fact", 8),
            KeyIndex = [0]
        }), 13), LateMaterializationMode.Auto).WithStateOptions(new StateManagerOptions());

        await using var stream = builder.Build();
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
    }

    [Fact]
    public async Task ReviewerCAutoPreservesBuildableUnserializablePlans()
    {
        Plan Rooted()
        {
            var plan = Write(FourJoins(Wide("fact", 8)), 13);
            plan.Relations[0] = new RootRelation()
            {
                Input = plan.Relations[0],
                Names = Enumerable.Range(0, 13).Select(i => "c" + i).ToList(),
                Emit = Enumerable.Range(0, 13).ToList()
            };
            return plan;
        }

        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        await using var off = RunnableBuilder(Rooted(), LateMaterializationMode.Off)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage }).Build();
        await using var auto = RunnableBuilder(Rooted(), LateMaterializationMode.Auto)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage }).Build();
    }

    [Fact]
    public async Task ReviewerCAdmittedStoreHasRoomForEveryFetchReader()
    {
        var plan = new Plan() { Relations = [Wide("fact", 8)] };
        for (int i = 0; i < 19; i++)
        {
            var input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 9 };
            plan.Relations.Add(Write(JoinedTo(input, "d" + i), 10).Relations[0]);
        }
        var (report, connectors, functions) = Analyze(plan);
        Assert.True(Of(report, "fact").Admitted, report.ToString());
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection()));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.All(plan.Relations.Skip(1), root => Assert.IsType<FetchRequiredColumnsRelation>(((WriteRelation)root).Input));

        using var storage = await RowHeapTests.Open(cachePages: 720);
        var read = Assert.IsType<ReadRelation>(plan.Relations[0]);
        var layout = RowStoreHost.ReadLayout(read.RowStore, 9, [0], Enumerable.Range(0, 9).ToList())!;
        int queue = FlowtideBuilder.RowStoreWriteQueuePages(720, 1, 19);
        Assert.Equal(15, queue);
        using var host = await RowStoreHost.CreateAsync(storage.Client, "keys", "rows", layout,
            new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { WriteQueueCapacity = queue } }, GlobalMemoryManager.Instance);
        Assert.Equal(queue + 2, storage.Manager.CacheTable.ReservedHeldPages);
        var readers = new List<RowHeapReader>();
        try
        {
            for (int i = 0; i < 19; i++)
            {
                readers.Add(host.Store.CreateReader());
            }
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader.Dispose();
            }
        }
    }

    [Fact]
    public void ReviewerCDisjointBranchesChargeEveryReferenceCopy()
    {
        var read = Wide("fact", 6, new StringType());
        read.BaseSchema.Struct!.Types[6] = new AnyType();
        Relation Branch(params int[] payload)
        {
            return JoinedTo(new ProjectRelation()
            {
                Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 7 },
                Expressions = [],
                Emit = new[] { 0 }.Concat(payload).ToList()
            }, "d" + payload[0]);
        }
        var plan = new Plan()
        {
            Relations = [read, Write(Branch(1, 2, 3), 5).Relations[0], Write(Branch(4, 5, 6), 5).Relations[0]]
        };
        var (report, connectors, functions) = Analyze(plan);
        var fact = Of(report, "fact");
        Assert.Equal(176, fact.Saving);
        Assert.Equal(2, fact.Fetches);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1,
            new Dictionary<Relation, List<int>>() { [read] = fact.Columns }));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.Equal(72, fact.Cost);
        Assert.False(fact.Admitted);
    }

    [Fact]
    public void ReviewerCUnionKeepsDeferredPayloadWhenPlainOriginsDiffer()
    {
        var read = Wide("fact", 9);
        FilterRelation Branch(string comparison, int bound) => new()
        {
            Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 10 },
            Condition = Compare(comparison, Field(1), new NumericLiteral() { Value = bound })
        };
        var union = new SetRelation()
        {
            Operation = SetOperation.UnionAll,
            Inputs = [Branch(FunctionsComparison.GreaterThan, 0), Branch(FunctionsComparison.LessThan, 10)]
        };
        var plan = Write(FourJoins(union), 14);
        plan.Relations.Insert(0, read);
        var (report, connectors, functions) = Analyze(plan);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.True(Of(report, "fact").Admitted, report.ToString());
    }

    [Fact]
    public async Task ReviewerCAdmissionReadsTheEffectiveCacheOptions()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var options = new StateManagerOptions() { PersistentStorage = storage, CachePageCount = 10000 };
        var builder = RunnableBuilder(Write(FourJoins(Wide("fact", 8)), 13), LateMaterializationMode.Auto)
            .WithStateOptions(options);
        options.CachePageCount = 40;
        options.MinCachePageCount = 40;

        await using var stream = builder.Build();
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
    }

    [Fact]
    public void ReviewerCMultiJoinCountsEachInputOfTheSameSource()
    {
        ReferenceRelation Reference() => new() { RelationId = 0, ReferenceOutputLength = 9 };
        var first = new MergeJoinRelation()
        {
            Left = Reference(), Right = Reference(), Type = JoinType.Inner,
            LeftKeys = [Field(0)], RightKeys = [Field(9)]
        };
        var second = new MergeJoinRelation()
        {
            Left = first, Right = Reference(), Type = JoinType.Inner,
            LeftKeys = [Field(0)], RightKeys = [Field(18)]
        };
        var plan = Write(second, 27);
        plan.Relations.Insert(0, Wide("fact", 8));
        Assert.Equal(1, MultiJoinMarker.Mark(plan, 8));
        var (report, connectors, functions) = Analyze(plan);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.Equal(3, Of(report, "fact").Copies);
    }
}
