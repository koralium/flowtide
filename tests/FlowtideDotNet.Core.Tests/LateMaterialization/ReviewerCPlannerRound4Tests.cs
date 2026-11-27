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
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
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

public class ReviewerCPlannerRound4Tests
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
            => ["k"];
    }

    private sealed class Sinks : RegexConnectorSinkFactory
    {
        public Sinks() : base("*")
        {
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private static int Run(Plan plan, int parallelism = 1)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        int stores = LateMaterializationPass.Run(plan, connectors, functions, parallelism);
        RowReferencePlanValidator.Validate(plan.Relations, functions, parallelism);
        return stores;
    }

    // A reader of an alias above a partitioned join
    private static Plan ReaderAboveAliasingProject(int partitions, bool emitOnReader)
    {
        var plan = Join(false, JoinType.Inner);
        var write = (WriteRelation)plan.Relations[0];
        var alias = new ProjectRelation()
        {
            Input = write.Input,
            Expressions = [Field(2)],
            Emit = [0, 4, 8]
        };
        var reader = new ProjectRelation()
        {
            Input = alias,
            Expressions = [Compare(FunctionsComparison.GreaterThan, Field(2), new StringLiteral() { Value = "l2" })]
        };
        if (emitOnReader)
        {
            reader.Emit = [0, 1, 3];
        }
        write.Input = reader;
        write.TableSchema = new NamedStruct() { Names = emitOnReader ? ["c0", "c1", "c2"] : ["c0", "c1", "c2", "c3"] };
        return partitions > 1 ? ParallelizeOptimizer.Optimize(plan, partitions) : plan;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AReaderAboveAnAliasFetchesOnlyTheAliasedColumnInEachPartition(bool emitOnReader)
    {
        var plan = ReaderAboveAliasingProject(3, emitOnReader);
        Assert.Equal(2, Run(plan));
        var reader = Assert.IsType<ProjectRelation>(((WriteRelation)plan.Relations[0]).Input);
        var alias = Assert.IsType<ProjectRelation>(reader.Input);
        var union = Assert.IsType<SetRelation>(alias.Input);
        Assert.Equal(3, union.Inputs.Count);
        Assert.All(union.Inputs, x => Assert.Equal(1, Assert.IsType<FetchRequiredColumnsRelation>(x).Fetches.Sum(f => f.Columns.Count)));
    }

    private static DifferentialTable[] JoinTables()
    {
        return
        [
            Table("l", Enumerable.Range(1, 30).Select(x => Row(x, x % 6, "l" + x, x % 4 == 0 ? null : "n" + x)).ToArray()),
            Table("r", Enumerable.Range(0, 6).Select(x => Row(x, x * 2, "m" + x, x % 2 == 0 ? null : "o" + x)).ToArray())
        ];
    }

    [Theory]
    [InlineData(2, false)]
    [InlineData(3, true)]
    public async Task AReaderAboveAnAliasMatchesOff(int partitions, bool emitOnReader)
    {
        await using var pair = DifferentialPair.Planned(() => ReaderAboveAliasingProject(partitions, emitOnReader), JoinTables);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["l"].Upsert(Row(2, 3, "z2", "b"), Row(31, 1, "a31", null)));
        pair.Change(tables => tables["r"].Upsert(Row(3, 7, "a3", "q")));
        pair.Change(tables => tables["r"].Delete(1));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());

        await pair.RestartAsync();
        pair.Change(tables => tables["l"].Delete(5, 6));
        pair.Change(tables => tables["l"].Upsert(Row(7, 1, "a7", "p")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }

    [Theory]
    [InlineData(ExecutionMode.OnCheckpoint)]
    [InlineData(ExecutionMode.OnWatermark)]
    public async Task AKeyedSinkAboveAnAliasMatchesOff(ExecutionMode mode)
    {
        await using var pair = DifferentialPair.Planned(
            () => ReaderAboveAliasingProject(2, false),
            JoinTables,
            null,
            () => new DifferentialDestination() { Mode = mode });
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());

        pair.Change(tables => tables["l"].Upsert(Row(2, 3, "z2", "b"), Row(31, 1, "a31", null)));
        pair.Change(tables => tables["r"].Delete(1));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());

        await pair.RestartAsync();
        pair.Change(tables => tables["l"].Upsert(Row(7, 1, "a7", "p")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }

    // A project over a project whose emit drops a computed column
    private static Plan ThreeProjectsOverAPartitionedJoin(int partitions)
    {
        var plan = Join(false, JoinType.Inner);
        var write = (WriteRelation)plan.Relations[0];
        var first = new ProjectRelation()
        {
            Input = write.Input,
            Expressions = [Field(6), Compare(FunctionsComparison.GreaterThan, Field(3), new StringLiteral() { Value = "n" })],
            Emit = [0, 8, 9, 5]
        };
        var second = new ProjectRelation()
        {
            Input = first,
            Expressions = [Field(1)],
            Emit = [0, 4, 3]
        };
        var third = new ProjectRelation()
        {
            Input = second,
            Expressions = [new StringLiteral() { Value = "x" }]
        };
        write.Input = third;
        write.TableSchema = new NamedStruct() { Names = ["c0", "c1", "c2", "c3"] };
        return partitions > 1 ? ParallelizeOptimizer.Optimize(plan, partitions) : plan;
    }

    [Fact]
    public void ThreeProjectsFetchWhatTheTopKeepsInEachPartition()
    {
        var plan = ThreeProjectsOverAPartitionedJoin(2);
        Assert.Equal(2, Run(plan));
        var third = Assert.IsType<ProjectRelation>(((WriteRelation)plan.Relations[0]).Input);
        var second = Assert.IsType<ProjectRelation>(third.Input);
        var first = Assert.IsType<ProjectRelation>(second.Input);
        var union = Assert.IsType<SetRelation>(first.Input);
        Assert.All(union.Inputs, x => Assert.Equal(3, Assert.IsType<FetchRequiredColumnsRelation>(x).Fetches.Sum(f => f.Columns.Count)));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task ThreeProjectsMatchOff(int partitions)
    {
        await using var pair = DifferentialPair.Planned(() => ThreeProjectsOverAPartitionedJoin(partitions), JoinTables);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["l"].Upsert(Row(2, 3, "z2", null), Row(31, 1, "a31", "zz")));
        pair.Change(tables => tables["r"].Upsert(Row(3, 7, "a3", "q")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());

        await pair.RestartAsync();
        pair.Change(tables => tables["r"].Delete(1));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }

    // A filter on an alias above a partitioned join
    private static Plan FilterOnAnAlias(int partitions)
    {
        var plan = Join(false, JoinType.Inner);
        var write = (WriteRelation)plan.Relations[0];
        var alias = new ProjectRelation()
        {
            Input = write.Input,
            Expressions = [Field(2)],
            Emit = [0, 4, 8, 7]
        };
        write.Input = new FilterRelation()
        {
            Input = alias,
            Condition = Compare(FunctionsComparison.GreaterThan, Field(2), new StringLiteral() { Value = "l2" })
        };
        write.TableSchema = new NamedStruct() { Names = ["c0", "c1", "c2", "c3"] };
        return partitions > 1 ? ParallelizeOptimizer.Optimize(plan, partitions) : plan;
    }

    [Fact]
    public void AFilterOnAnAliasFetchesOnlyItsConditionBeforeTheMerge()
    {
        var plan = FilterOnAnAlias(2);
        Assert.Equal(2, Run(plan));
        var fetch = Assert.IsType<FetchRequiredColumnsRelation>(((WriteRelation)plan.Relations[0]).Input);
        var filter = Assert.IsType<FilterRelation>(fetch.Input);
        var alias = Assert.IsType<ProjectRelation>(filter.Input);
        var union = Assert.IsType<SetRelation>(alias.Input);
        Assert.Equal(2, union.Inputs.Count);
        Assert.All(union.Inputs, x => Assert.Equal(1, Assert.IsType<FetchRequiredColumnsRelation>(x).Fetches.Sum(f => f.Columns.Count)));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task AFilterOnAnAliasMatchesOff(int partitions)
    {
        await using var pair = DifferentialPair.Planned(() => FilterOnAnAlias(partitions), JoinTables);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["l"].Upsert(Row(2, 3, "z2", "b"), Row(31, 1, "a31", null)));
        pair.Change(tables => tables["r"].Upsert(Row(3, 7, "a3", "q")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());

        await pair.RestartAsync();
        pair.Change(tables => tables["r"].Delete(1));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }

    // A gather exchange to another substream over a union of one store
    private static Plan GatherOverAUnion()
    {
        Relation Branch(string comparison, int bound)
        {
            return new FilterRelation()
            {
                Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 4 },
                Condition = Compare(comparison, Field(1), new NumericLiteral() { Value = bound })
            };
        }
        var union = new SetRelation() { Operation = SetOperation.UnionAll, Inputs = [Branch(FunctionsComparison.GreaterThan, 2), Branch(FunctionsComparison.LessThan, 4)] };
        var exchange = new ExchangeRelation()
        {
            Input = union,
            ExchangeKind = new ScatterExchangeKind() { Fields = [] },
            PartitionCount = 1,
            Targets = [new SubstreamExchangeTarget() { ExchangeTargetId = 0, PartitionIds = [], SubstreamName = "s1" }]
        };
        return new Plan()
        {
            Relations =
            [
                Read("t", false),
                new SubStreamRootRelation() { Name = "s0", Input = exchange }
            ]
        };
    }

    [Fact]
    public void AGatherExchangeOverAUnionFetchesInEachInput()
    {
        var plan = GatherOverAUnion();
        Assert.Equal(1, Run(plan));
        var exchange = Assert.IsType<ExchangeRelation>(((SubStreamRootRelation)plan.Relations[1]).Input);
        var union = Assert.IsType<SetRelation>(exchange.Input);
        Assert.All(union.Inputs, x => Assert.IsType<FetchRequiredColumnsRelation>(x));
    }
}
