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
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ParallelJoinExchangeKeyTests
{
    private sealed class Sources : RegexConnectorSourceFactory
    {
        public Sources() : base("*") { }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => ["k"];
    }

    private sealed class Sinks : RegexConnectorSinkFactory
    {
        public Sinks() : base("*") { }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    [Fact]
    public void ParallelJoinWithPayloadPostFilterRewritesSharedExchangeKeySafely()
    {
        var plan = Join(false, JoinType.Inner);
        var write = Assert.IsType<WriteRelation>(plan.Relations[0]);
        var join = Assert.IsType<MergeJoinRelation>(write.Input);
        join.PostJoinFilter = new ScalarFunction()
        {
            ExtensionUrn = FunctionsBoolean.Urn,
            ExtensionName = FunctionsBoolean.And,
            Arguments =
            [
                Compare(FunctionsComparison.Equal, Field(2), Field(6)),
                Compare(FunctionsComparison.Equal, Field(3), Field(7))
            ]
        };
        ParallelizeOptimizer.Optimize(plan, 3);
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(2, LateMaterializationPass.Run(plan, connectors, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.Equal(8, write.Input.OutputLength);
    }

    [Fact]
    public void ExchangeReferenceWithIdentityEmitKeepsTheExchangeAsItsTarget()
    {
        var plan = Write(new StandardOutputExchangeReferenceRelation()
        {
            RelationId = 1,
            TargetId = 0,
            ReferenceOutputLength = 4,
            Emit = [0, 1, 2, 3]
        }, 4);
        plan.Relations.Add(new ExchangeRelation()
        {
            Input = Read("t", false),
            ExchangeKind = new BroadcastExchangeKind(),
            Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }],
            PartitionCount = 1
        });
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        var exchange = Assert.IsType<ExchangeRelation>(plan.Relations[1]);
        Assert.Equal(4, exchange.Input.OutputLength);
    }

    [Fact]
    public async Task ExchangeReferenceWithIdentityEmitCompilesInBothModes()
    {
        Plan CreatePlan()
        {
            var plan = Write(new StandardOutputExchangeReferenceRelation()
            {
                RelationId = 1,
                TargetId = 0,
                ReferenceOutputLength = 4,
                Emit = [0, 1, 2, 3]
            }, 4);
            plan.Relations.Add(new ExchangeRelation()
            {
                Input = Read("t", false),
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }],
                PartitionCount = 1
            });
            return plan;
        }

        await using var pair = DifferentialPair.Planned(CreatePlan, () => [Table("t", Row(1, 5, "a", "x"))]);
        await pair.Plain.StartAsync();
        await pair.Hosted.StartAsync();
        await pair.Expect("1|5|a|x");
    }
}
