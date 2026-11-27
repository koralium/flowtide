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
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class PostFilterJoinTests
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

    private static FunctionsRegister Functions()
    {
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        functions.RegisterScalarFunctionWithExpression(
            "extension:test:rows", "is_odd", x => FlexBuffers.FlxValue.FromBytes(FlexBuffers.FlexBuffer.SingleValue(x.AsLong % 2 == 1)));
        return functions;
    }

    private static int Run(Plan plan, int parallelism = 1)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = Functions();
        int stores = LateMaterializationPass.Run(plan, connectors, functions, parallelism);
        RowReferencePlanValidator.Validate(plan.Relations, functions, parallelism);
        return stores;
    }

    private static Expression PayloadFilter()
    {
        return new ScalarFunction()
        {
            ExtensionUrn = FunctionsBoolean.Urn,
            ExtensionName = FunctionsBoolean.Or,
            Arguments = [Compare(FunctionsComparison.GreaterThan, Field(2), Field(6)), Compare(FunctionsComparison.GreaterThan, Field(3), Field(7))]
        };
    }

    private static Plan PartitionedJoin(JoinType type, bool rowOnly)
    {
        var plan = Join(false, type);
        var join = (MergeJoinRelation)((WriteRelation)plan.Relations[0]).Input;
        join.PostJoinFilter = rowOnly
            ? new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "is_odd", Arguments = [Field(0)] }
            : PayloadFilter();
        return Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(plan, 2);
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    public void APartitionedJoinWithAPostFilterStillGetsStores(bool rowOnly, int parallelism)
    {
        var plan = PartitionedJoin(JoinType.Inner, rowOnly);
        int stores = Run(plan, parallelism);
        Assert.Equal(2, stores);
        var exchanges = plan.Relations.OfType<ExchangeRelation>().ToList();
        Assert.Equal(2, exchanges.Count);
        Assert.Single(exchanges, x => x.Input is FetchRequiredColumnsRelation);
        Assert.Single(exchanges, x => x.Input is ReadRelation read && read.RowStore != null);
    }

    [Theory]
    [InlineData(JoinType.Inner, false, 1)]
    [InlineData(JoinType.Left, false, 1)]
    [InlineData(JoinType.Outer, false, 1)]
    [InlineData(JoinType.Left, true, 1)]
    [InlineData(JoinType.Inner, false, 2)]
    public async Task APartitionedJoinWithAPostFilterMatchesOff(JoinType type, bool rowOnly, int parallelism)
    {
        await using var pair = DifferentialPair.Planned(
            () => PartitionedJoin(type, rowOnly),
            () =>
            [
                Table("l", Row(1, 10, "l1", "a"), Row(2, 20, "l2", null), Row(3, 30, "l3", "c"), Row(5, 10, "z5", "q")),
                Table("r", Row(10, 1, "r10", "x"), Row(20, 2, "a20", "y"), Row(30, 3, "a30", null), Row(40, 4, "r40", null))
            ],
            builder =>
            {
                builder.SetParallelism(parallelism);
                if (rowOnly)
                {
                    RegisterRowsOnly(builder);
                }
            });
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.NotEmpty(pair.Hosted.Result());
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);

        pair.Change(tables => tables["l"].Upsert(Row(2, 30, "z2", "b"), Row(4, 10, "l4", null)));
        pair.Change(tables => tables["r"].Delete(10));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());

        await pair.RestartAsync();
        pair.Change(tables => tables["r"].Upsert(Row(10, 9, "a10", "z"), Row(50, 5, "r50", null)));
        pair.Change(tables => tables["l"].Delete(1));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }
    // a joined with b under a row-only condition, then with c
    private static Plan GroupWithInnerRowOnlyCondition()
    {
        var first = new MergeJoinRelation()
        {
            Left = Read("a", false),
            Right = Read("b", false),
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(4)],
            PostJoinFilter = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "is_odd", Arguments = [Field(1)] }
        };
        var second = new MergeJoinRelation()
        {
            Left = first,
            Right = Read("c", false),
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(8)]
        };
        var plan = Write(second, 12);
        Assert.Equal(1, Core.Operators.Join.MultiJoin.MultiJoinMarker.Mark(plan, 8));
        return plan;
    }

    [Fact]
    public void ARowOnlyConditionOnAnInnerJoinOfAGroupMakesEveryInputPlain()
    {
        var plan = GroupWithInnerRowOnlyCondition();
        Assert.True(Run(plan) > 0);
        var second = Assert.IsType<MergeJoinRelation>(((WriteRelation)plan.Relations[0]).Input);
        Assert.Equal(4, second.Right.OutputLength);
    }

    [Fact]
    public async Task ARowOnlyConditionOnAnInnerJoinOfAGroupMatchesOff()
    {
        await using var pair = DifferentialPair.Planned(
            GroupWithInnerRowOnlyCondition,
            () =>
            [
                Table("a", Row(1, 5, "a1", "n1"), Row(2, 6, "a2", "n2"), Row(3, 7, "a3", null)),
                Table("b", Row(1, 10, "b1", "m1"), Row(2, 20, "b2", null), Row(3, 30, "b3", "m3")),
                Table("c", Row(1, 100, "c1", "o1"), Row(2, 200, "c2", "o2"), Row(3, 300, null, "o3"))
            ],
            RegisterRowsOnly);
        await pair.StartAsync();
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
        Assert.Equal(2, pair.Hosted.Result().Count);

        pair.Change(tables => tables["c"].Upsert(Row(1, 101, "c1b", "o1b")));
        pair.Change(tables => tables["a"].Upsert(Row(2, 9, "a2b", "n2b")));
        await pair.SettleBoth();
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }
    [Fact]
    public void APartitionedJoinUnderAProjectFetchesBeforeTheMergingUnion()
    {
        var plan = Join(false, JoinType.Inner);
        var write = (WriteRelation)plan.Relations[0];
        var project = new ProjectRelation()
        {
            Input = write.Input,
            Expressions = [Compare(FunctionsComparison.GreaterThan, Field(2), Field(6))],
            Emit = [0, 4, 8]
        };
        write.Input = project;
        write.TableSchema = new FlowtideDotNet.Substrait.Type.NamedStruct() { Names = ["c0", "c1", "c2"] };
        plan = Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(plan, 2);
        Assert.Equal(2, Run(plan));

        var top = Assert.IsType<ProjectRelation>(((WriteRelation)plan.Relations[0]).Input);
        var union = Assert.IsType<SetRelation>(top.Input);
        Assert.All(union.Inputs, x => Assert.IsType<FetchRequiredColumnsRelation>(x));
    }
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ANumberedReadGetsItsStoreWhicheverRootIsVisitedFirst(bool readFirst)
    {
        var read = Read("t", false);
        var write = new WriteRelation()
        {
            Input = new NormalizationRelation() { Input = new ReferenceRelation() { RelationId = readFirst ? 0 : 1, ReferenceOutputLength = 4 }, KeyIndex = [0] },
            NamedObject = new FlowtideDotNet.Substrait.Type.NamedTable() { Names = ["output"] },
            TableSchema = new FlowtideDotNet.Substrait.Type.NamedStruct() { Names = ["c0", "c1", "c2", "c3"] }
        };
        var plan = new Plan() { Relations = readFirst ? [read, write] : [write, read] };
        Assert.Equal(2, Run(plan));
        Assert.NotNull(read.RowStore);
    }
}
