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

public class ReviewerCPlannerRound3Tests
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

    private static Plan ProjectOverPartitionedJoin(int partitions)
    {
        var plan = Join(false, JoinType.Inner);
        var write = (WriteRelation)plan.Relations[0];
        write.Input = new ProjectRelation()
        {
            Input = write.Input,
            Expressions = [Compare(FunctionsComparison.GreaterThan, Field(2), Field(6))],
            Emit = [0, 4, 8]
        };
        write.TableSchema = new NamedStruct() { Names = ["c0", "c1", "c2"] };
        return ParallelizeOptimizer.Optimize(plan, partitions);
    }

    [Fact]
    public void APartitionedJoinUnderAProjectFetchesOnlyWhatTheProjectKeeps()
    {
        var plan = ProjectOverPartitionedJoin(2);
        Assert.Equal(2, Run(plan));
        var top = Assert.IsType<ProjectRelation>(((WriteRelation)plan.Relations[0]).Input);
        var union = Assert.IsType<SetRelation>(top.Input);
        Assert.All(union.Inputs, x => Assert.Equal(2, Assert.IsType<FetchRequiredColumnsRelation>(x).Fetches.Sum(f => f.Columns.Count)));
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
    [InlineData(2)]
    [InlineData(3)]
    public async Task APartitionedJoinUnderAProjectMatchesOff(int partitions)
    {
        await using var pair = DifferentialPair.Planned(() => ProjectOverPartitionedJoin(partitions), JoinTables);
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
        pair.Change(tables => tables["r"].Upsert(Row(1, 9, "zz", "p")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }

    // Two branches of one store, merged, optionally reordered by an emit
    private static Relation UnionOfOneStore(List<int>? emit)
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
        if (emit != null)
        {
            union.Emit = emit;
        }
        return union;
    }

    // The union is the left input of a join on text
    private static Plan JoinOnAUnion(int partitions)
    {
        var join = new MergeJoinRelation()
        {
            Left = UnionOfOneStore(null),
            Right = Read("r", false),
            Type = JoinType.Left,
            LeftKeys = [Field(2)],
            RightKeys = [Field(6)]
        };
        var plan = Write(join, 8);
        plan.Relations.Insert(0, Read("t", false));
        return partitions > 1 ? ParallelizeOptimizer.Optimize(plan, partitions) : plan;
    }

    private static DifferentialTable[] UnionTables()
    {
        return
        [
            Table("t", Enumerable.Range(1, 12).Select(x => Row(x, x % 6, "s" + (x % 5), x % 3 == 0 ? null : "n" + x)).ToArray()),
            Table("r", Enumerable.Range(0, 5).Select(x => Row(x, x, "s" + x, x % 2 == 0 ? null : "o" + x)).ToArray())
        ];
    }

    [Fact]
    public void AnExchangeOverAUnionFetchesWhatItRoutesOnInEachInput()
    {
        var plan = JoinOnAUnion(2);
        Assert.True(Run(plan) > 0);
        var exchange = Assert.Single(plan.Relations.OfType<ExchangeRelation>(), x => x.Input is SetRelation);
        var union = (SetRelation)exchange.Input;
        Assert.All(union.Inputs, x => Assert.IsType<FetchRequiredColumnsRelation>(x));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task AJoinOnAUnionMatchesOff(int partitions)
    {
        await using var pair = DifferentialPair.Planned(() => JoinOnAUnion(partitions), UnionTables);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["t"].Upsert(Row(2, 3, "s4", "b"), Row(13, 1, "s1", null)));
        pair.Change(tables => tables["r"].Upsert(Row(3, 7, "s2", "q")));
        pair.Change(tables => tables["t"].Delete(4));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());

        await pair.RestartAsync();
        pair.Change(tables => tables["r"].Delete(1));
        pair.Change(tables => tables["t"].Upsert(Row(5, 2, "s0", "c")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }

    // A filter on text above a union whose emit puts text first
    private static Plan FilterOnAReorderedUnion()
    {
        var filter = new FilterRelation()
        {
            Input = UnionOfOneStore([2, 0, 3, 1]),
            Condition = Compare(FunctionsComparison.GreaterThan, Field(0), new StringLiteral() { Value = "s1" })
        };
        var plan = Write(filter, 4);
        plan.Relations.Insert(0, Read("t", false));
        return plan;
    }

    [Fact]
    public void AFilterAboveAReorderedUnionFetchesItsColumnInEachInput()
    {
        var plan = FilterOnAReorderedUnion();
        Assert.True(Run(plan) > 0);
        var fetch = Assert.IsType<FetchRequiredColumnsRelation>(((WriteRelation)plan.Relations[1]).Input);
        var filter = Assert.IsType<FilterRelation>(fetch.Input);
        var union = Assert.IsType<SetRelation>(filter.Input);
        Assert.All(union.Inputs, x => Assert.Equal(1, Assert.IsType<FetchRequiredColumnsRelation>(x).Fetches.Sum(f => f.Columns.Count)));
    }

    [Fact]
    public async Task AFilterAboveAReorderedUnionMatchesOff()
    {
        await using var pair = DifferentialPair.Planned(FilterOnAReorderedUnion, UnionTables);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["t"].Upsert(Row(2, 3, "s0", "b"), Row(13, 1, "s4", null)));
        pair.Change(tables => tables["t"].Delete(4));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());

        await pair.RestartAsync();
        pair.Change(tables => tables["t"].Upsert(Row(5, 2, "s9", "c")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }
    // A union of the two branches and a third branch
    private static Plan UnionInsideAUnion()
    {
        var third = new FilterRelation()
        {
            Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 4 },
            Condition = Compare(FunctionsComparison.GreaterThan, Field(1), new NumericLiteral() { Value = 4 })
        };
        var outer = new SetRelation() { Operation = SetOperation.UnionAll, Inputs = [UnionOfOneStore(null), third] };
        var plan = Write(outer, 4);
        plan.Relations.Insert(0, Read("t", false));
        return plan;
    }

    [Fact]
    public void AUnionInsideAUnionFetchesBeforeItsOwnMerge()
    {
        var plan = UnionInsideAUnion();
        Assert.True(Run(plan) > 0);
        var outer = Assert.IsType<SetRelation>(((WriteRelation)plan.Relations[1]).Input);
        var inner = Assert.IsType<SetRelation>(outer.Inputs[0]);
        Assert.All(inner.Inputs, x => Assert.IsType<FetchRequiredColumnsRelation>(x));
    }

    [Fact]
    public async Task AUnionInsideAUnionMatchesOff()
    {
        await using var pair = DifferentialPair.Planned(UnionInsideAUnion, UnionTables);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["t"].Upsert(Row(2, 5, "s0", "b"), Row(13, 1, "s4", null)));
        pair.Change(tables => tables["t"].Delete(4));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Empty(pair.Hosted.Negative());
    }
    private static int PassOnly(Plan plan)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        return LateMaterializationPass.Run(plan, connectors, functions, 1);
    }

    [Fact]
    public void AnOuterReferenceInAProjectIsNotTakenForALocalColumn()
    {
        var project = new ProjectRelation()
        {
            Input = Read("t", false),
            Expressions = [new DirectFieldReference() { Root = new OuterReference() { StepsOut = 1 }, ReferenceSegment = new StructReferenceSegment() { Field = 2 } }]
        };
        var plan = Write(project, 5);
        Assert.Equal(1, PassOnly(plan));
        var kept = Assert.IsType<DirectFieldReference>(Assert.Single(project.Expressions));
        Assert.Equal(1u, Assert.IsType<OuterReference>(kept.Root).StepsOut);
        Assert.Equal(2, Assert.IsType<StructReferenceSegment>(kept.ReferenceSegment).Field);
    }

    [Fact]
    public void AFieldInsideAnArrayLiteralMovesWithItsColumn()
    {
        var project = new ProjectRelation()
        {
            Input = Read("t", false),
            Expressions = [new ArrayLiteral() { Expressions = [Field(1)] }]
        };
        var plan = Write(project, 5);
        Assert.Equal(1, PassOnly(plan));
        var array = Assert.IsType<ArrayLiteral>(Assert.Single(project.Expressions));
        var field = Assert.IsType<DirectFieldReference>(Assert.Single(array.Expressions));
        Assert.Equal(2, Assert.IsType<StructReferenceSegment>(field.ReferenceSegment).Field);
    }
}

