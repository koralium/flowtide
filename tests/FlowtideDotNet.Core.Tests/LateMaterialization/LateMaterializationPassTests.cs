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

public class LateMaterializationPassTests
{
    private sealed class Sources(bool hosts) : RegexConnectorSourceFactory("*")
    {
        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => hosts ? ["k"] : null;
    }

    private sealed class Sinks(IReadOnlyList<int>? keys) : RegexConnectorSinkFactory("*")
    {
        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation)
            => keys;
    }

    private static int Run(Plan plan, bool hosts = true, IReadOnlyList<int>? sinkKeys = null, int parallelism = 1)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources(hosts));
        connectors.AddSink(new Sinks(sinkKeys));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        int stores = LateMaterializationPass.Run(plan, connectors, functions, parallelism);
        RowReferencePlanValidator.Validate(plan.Relations, functions, parallelism);
        var json = SubstraitSerializer.SerializeToJson(plan);
        var back = SubstraitDeserializer.DeserializeFromJson(json);
        Assert.Equal(plan, back);
        Assert.Equal(json, SubstraitSerializer.SerializeToJson(back));
        return stores;
    }

    private static WriteRelation WriteOf(Plan plan)
    {
        return plan.Relations.OfType<WriteRelation>().Single();
    }

    private static int FieldOf(Expression expression)
    {
        return ((StructReferenceSegment)((DirectFieldReference)expression).ReferenceSegment).Field;
    }

    [Fact]
    public void EveryColumnButTheKeyIsStoredAndFetchedBeforeItsFirstReader()
    {
        var plan = FilterAndProject(false);
        Assert.Equal(1, Run(plan));

        var write = WriteOf(plan);
        var last = Assert.IsType<FetchRequiredColumnsRelation>(write.Input);
        Assert.Equal(5, last.OutputLength);
        var project = Assert.IsType<ProjectRelation>(last.Input);
        var filter = Assert.IsType<FilterRelation>(project.Input);
        var first = Assert.IsType<FetchRequiredColumnsRelation>(filter.Input);
        var read = Assert.IsType<ReadRelation>(first.Input);
        Assert.Equal([1, 2, 3], read.RowStore!.StoredColumns);
        Assert.Equal(2, read.OutputLength);
        Assert.Equal([0], Assert.Single(first.Fetches).Columns);
        Assert.Equal([1, 2], Assert.Single(last.Fetches).Columns);
        Assert.Equal(2, FieldOf(((ScalarFunction)filter.Condition).Arguments[0]));
    }

    [Fact]
    public void ASourceThatDeclaresNoKeysIsLeftAlone()
    {
        var plan = FilterAndProject(false);
        Assert.Equal(0, Run(plan, hosts: false));
        Assert.Equal(FilterAndProject(false), plan);
    }

    [Fact]
    public void APlanWithStoresPlacedByHandIsLeftAlone()
    {
        var plan = FilterAndProject(true);
        Assert.Equal(0, Run(plan));
        Assert.Equal(FilterAndProject(true), plan);
    }

    [Fact]
    public void ANormalizationHostsTheStoreAndItsReadDoesNot()
    {
        var read = Read("t", false);
        var normalization = new NormalizationRelation() { Input = read, KeyIndex = [0], Emit = [3, 0, 1] };
        var plan = Write(normalization, 3);
        Assert.Equal(1, Run(plan));

        Assert.Null(read.RowStore);
        Assert.Equal([0, 2], normalization.RowStore!.StoredColumns);
        var fetch = Assert.IsType<FetchRequiredColumnsRelation>(WriteOf(plan).Input);
        Assert.Same(normalization, fetch.Input);
        Assert.Equal([2, 0, 3], fetch.Emit);
    }

    [Fact]
    public void ARelationThatReadsWholeRowsGetsPlainValues()
    {
        var aggregate = new AggregateRelation()
        {
            Input = Read("t", false),
            Groupings = [new AggregateGrouping() { GroupingExpressions = [Field(2)] }]
        };
        var plan = Write(aggregate, 1);
        Assert.Equal(1, Run(plan));

        var fetch = Assert.IsType<FetchRequiredColumnsRelation>(aggregate.Input);
        Assert.Equal(4, fetch.OutputLength);
        Assert.Equal([0, 2, 3, 4], fetch.Emit);
        Assert.Equal(2, FieldOf(aggregate.Groupings[0].GroupingExpressions[0]));
        Assert.Same(aggregate, WriteOf(plan).Input);
    }

    [Fact]
    public void TheKeysOfAMultiJoinAreFetchedUnderTheGroup()
    {
        var plan = ThreeTables(false, JoinType.Inner);
        Assert.Equal(3, Run(plan));

        var top = Assert.IsType<FetchRequiredColumnsRelation>(WriteOf(plan).Input);
        var second = Assert.IsType<MergeJoinRelation>(top.Input);
        var first = Assert.IsType<MergeJoinRelation>(second.Left);
        Assert.Equal(second.MultiJoinGroup, first.MultiJoinGroup);
        Assert.NotNull(first.MultiJoinGroup);
        Assert.IsType<FetchRequiredColumnsRelation>(first.Left);
        Assert.IsType<FetchRequiredColumnsRelation>(first.Right);
        Assert.IsType<ReadRelation>(second.Right);
        Assert.Equal(3, top.Fetches.Count);
        Assert.Equal(12, top.OutputLength);
    }

    [Fact]
    public void APlainJoinUnderBuilderParallelismGetsPlainValues()
    {
        var plan = Join(false, JoinType.Inner);
        Assert.Equal(2, Run(plan, parallelism: 4));

        var join = Assert.IsType<MergeJoinRelation>(WriteOf(plan).Input);
        Assert.Equal(4, Assert.IsType<FetchRequiredColumnsRelation>(join.Left).OutputLength);
        Assert.Equal(4, Assert.IsType<FetchRequiredColumnsRelation>(join.Right).OutputLength);
        Assert.Equal(1, FieldOf(join.LeftKeys[0]));
    }

    [Fact]
    public void ASinkThatDeclaresItsKeyGetsItAsAValueUnderItsOwnFetch()
    {
        var plan = Write(Read("t", false), 4);
        Assert.Equal(1, Run(plan, sinkKeys: [2]));

        var top = Assert.IsType<FetchRequiredColumnsRelation>(WriteOf(plan).Input);
        var below = Assert.IsType<FetchRequiredColumnsRelation>(top.Input);
        Assert.Equal([1], Assert.Single(below.Fetches).Columns);
        Assert.Equal([0, 2], Assert.Single(top.Fetches).Columns);
        Assert.True(top.Emit![2] < below.OutputLength);
        Assert.True(top.Emit![1] >= below.OutputLength);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TwoReadersOfOneRelationGetTheSameReferences(bool sinkFetches)
    {
        var plan = UnionOfTwoFilters(false);
        Assert.Equal(1, Run(plan, sinkKeys: sinkFetches ? [0] : null));

        var read = Assert.IsType<ReadRelation>(plan.Relations[0]);
        Assert.NotNull(read.RowStore);
        var input = WriteOf(plan).Input;
        if (sinkFetches)
        {
            input = Assert.IsType<FetchRequiredColumnsRelation>(input).Input;
        }
        var union = Assert.IsType<SetRelation>(input);
        foreach (var branch in union.Inputs)
        {
            var top = sinkFetches ? branch : Assert.IsType<FetchRequiredColumnsRelation>(branch).Input;
            var filter = Assert.IsType<FilterRelation>(top);
            var fetch = Assert.IsType<FetchRequiredColumnsRelation>(filter.Input);
            Assert.Equal(2, Assert.IsType<ReferenceRelation>(fetch.Input).ReferenceOutputLength);
        }
    }

    [Fact]
    public void ALeftMarkJoinKeepsItsMarkAfterTheLeftColumns()
    {
        var left = Read("l", false);
        var right = Read("r", false);
        var join = new MergeJoinRelation()
        {
            Left = left,
            Right = right,
            Type = JoinType.LeftMark,
            LeftKeys = [Field(1)],
            RightKeys = [Field(4)],
            Emit = [4, 2, 0]
        };
        var plan = Write(join, 3);
        Assert.Equal(2, Run(plan));

        var top = Assert.IsType<FetchRequiredColumnsRelation>(WriteOf(plan).Input);
        Assert.Same(join, top.Input);
        var keyed = Assert.IsType<FetchRequiredColumnsRelation>(join.Left);
        Assert.Equal(3, keyed.OutputLength);
        Assert.Equal([3, 1, 0], join.Emit);
        Assert.Equal([1], Assert.Single(top.Fetches).Columns);
        Assert.Equal(1, Assert.Single(top.Fetches).ReferenceColumn);
        Assert.Equal([0, 3, 2], top.Emit);
    }

    [Fact]
    public void AProjectOfOnlySecondNamesKeepsNoExpression()
    {
        var project = new ProjectRelation() { Input = Read("t", false), Expressions = [Field(2), Field(3)], Emit = [5, 0, 4] };
        var plan = Write(project, 3);
        Assert.Equal(1, Run(plan));

        Assert.Empty(project.Expressions);
        Assert.Equal([1, 0], project.Emit);
        var top = Assert.IsType<FetchRequiredColumnsRelation>(WriteOf(plan).Input);
        Assert.Equal([2, 1], Assert.Single(top.Fetches).Columns);
        Assert.Equal([2, 1, 3], top.Emit);
    }

    [Fact]
    public void AUnionOfTwoStoresGetsPlainValuesFromBoth()
    {
        var union = new SetRelation() { Operation = SetOperation.UnionAll, Inputs = [Read("a", false), Read("b", false)] };
        var plan = Write(union, 4);
        Assert.Equal(2, Run(plan, sinkKeys: [0]));

        Assert.Same(union, WriteOf(plan).Input);
        foreach (var input in union.Inputs)
        {
            Assert.Equal(4, Assert.IsType<FetchRequiredColumnsRelation>(input).OutputLength);
        }
    }

    [Fact]
    public void ARelationObjectInTwoPlacesLeavesThePlanAlone()
    {
        var shared = new FilterRelation() { Input = Read("t", false), Condition = Compare(FunctionsComparison.GreaterThan, Field(1), new NumericLiteral() { Value = 1 }) };
        var plan = Write(new SetRelation() { Operation = SetOperation.UnionAll, Inputs = [shared, shared] }, 4);
        Assert.Equal(0, Run(plan));
        Assert.Null(((ReadRelation)shared.Input).RowStore);
    }

    private static async Task<string> HashOf(LateMaterializationMode mode, bool byHand = false)
    {
        return (await VersionOf(mode, byHand)).Hash;
    }

    private static async Task<FlowtideDotNet.Storage.StreamVersionInformation> VersionOf(LateMaterializationMode mode, bool byHand = false)
    {
        await using var stream = new DifferentialStream(() => FilterAndProject(byHand), builder => builder.SetLateMaterialization(mode).SetVersionParts([(null, true)]), Table("t", Row(1, 5, "a", "x")));
        await stream.StartAsync();
        var context = typeof(Base.Engine.DataflowStream).GetField("streamContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(stream.Stream)!;
        return ((Base.Engine.Internal.StateMachine.StreamContext)context)._streamVersionInformation!;
    }

    [Fact]
    public async Task OnlyAModeThatPlacesAStoreChangesThePlanHash()
    {
        var off = await HashOf(LateMaterializationMode.Off);
        Assert.Equal(off, await HashOf(LateMaterializationMode.Placebo));
        Assert.Equal(off, await HashOf(LateMaterializationMode.Auto));
        Assert.NotEqual(off, await HashOf(LateMaterializationMode.ForceOn));
    }

    [Fact]
    public async Task TheVersionIsMadeFromTheHashOfThePlanWithItsStores()
    {
        var off = await VersionOf(LateMaterializationMode.Off);
        var on = await VersionOf(LateMaterializationMode.ForceOn);
        Assert.Equal(off.Hash, off.Version);
        Assert.Equal(on.Hash, on.Version);
        Assert.NotEqual(off.Version, on.Version);
    }

    [Fact]
    public async Task AModeThatPlacesNoStoreKeepsThePlanHash()
    {
        Assert.Equal(await HashOf(LateMaterializationMode.Off, true), await HashOf(LateMaterializationMode.ForceOn, true));
    }

    [Fact]
    public async Task APlanThatCannotBeSerializedIsRefusedOnceItHasAStore()
    {
        Plan Build()
        {
            var plan = Write(Read("t", false), 4);
            plan.Relations[0] = new RootRelation() { Input = plan.Relations[0], Names = ["k", "n", "text", "note"], Emit = [0, 1, 2, 3] };
            return plan;
        }
        await using var stream = new DifferentialStream(Build, builder => builder.SetLateMaterialization(LateMaterializationMode.ForceOn), Table("t", Row(1, 5, "a", "x")));

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(stream.StartAsync);
        Assert.Contains("has a row store and could not be serialized", refused.Message);
    }
}
