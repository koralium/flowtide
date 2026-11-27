using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ExactExchangeFetchTests
{
    private static Plan ExactExchangeAbovePartitionedJoin(bool normalization = true)
    {
        var plan = Join(false, JoinType.Inner);
        var write = (WriteRelation)plan.Relations[0];
        var join = (MergeJoinRelation)write.Input;
        if (normalization)
        {
            join.Left = new NormalizationRelation() { Input = join.Left, KeyIndex = [0] };
            join.Right = new NormalizationRelation() { Input = join.Right, KeyIndex = [0] };
        }
        ParallelizeOptimizer.Optimize(plan, 3);
        int exchangeIndex = plan.Relations.Count;
        var exchange = new ExchangeRelation()
        {
            Input = write.Input,
            ExchangeKind = new BroadcastExchangeKind(),
            Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }],
            PartitionCount = 1
        };
        plan.Relations.Add(exchange);
        write.Input = new StandardOutputExchangeReferenceRelation()
        {
            RelationId = exchangeIndex,
            TargetId = 0,
            ReferenceOutputLength = 8,
            Emit = Enumerable.Range(0, 8).ToList()
        };
        return plan;
    }

    [Fact]
    public void AnExactExchangeFetchesBeforeThePartitionMerge()
    {
        var plan = ExactExchangeAbovePartitionedJoin();
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(2, LateMaterializationPass.Run(plan, null, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        var exchange = Assert.IsType<ExchangeRelation>(plan.Relations[^1]);
        var union = Assert.IsType<SetRelation>(exchange.Input);
        Assert.Equal(3, union.Inputs.Count);
        Assert.All(union.Inputs, input =>
        {
            var fetch = Assert.IsType<FetchRequiredColumnsRelation>(input);
            Assert.IsType<MergeJoinRelation>(fetch.Input);
            Assert.Equal(8, fetch.OutputLength);
        });
    }

    [Fact]
    public async Task AnExactExchangeAbovePartitionedJoinRunsInBothModes()
    {
        await using var pair = DifferentialPair.Planned(() => ExactExchangeAbovePartitionedJoin(false),
            () => [Table("l", Row(1, 10, "left", "note")), Table("r", Row(10, 100, "right", "tail"))]);

        await pair.StartAsync();
        await pair.Expect("1|10|left|note|10|100|right|tail");
        Assert.True(Count(pair.Hosted, "Fetch required columns") > 0);
        Assert.Equal(0, Count(pair.Plain, "Fetch required columns"));
        pair.Change(tables => tables["l"].Upsert(Row(1, 10, "changed", "note")));
        await pair.Expect("1|10|changed|note|10|100|right|tail");
    }
}
