using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBPerformanceTests
{

    [Fact]
    public void ParallelJoinFetchesPayloadBeforeMergingItsPartitions()
    {
        var plan = Join(false, JoinType.Left);
        var write = Assert.IsType<WriteRelation>(plan.Relations[0]);
        var join = Assert.IsType<MergeJoinRelation>(write.Input);
        join.Left = new NormalizationRelation() { Input = join.Left, KeyIndex = [0] };
        join.Right = new NormalizationRelation() { Input = join.Right, KeyIndex = [0] };
        ParallelizeOptimizer.Optimize(plan, 3);
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(2, LateMaterializationPass.Run(plan, null, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        var union = Assert.IsType<SetRelation>(write.Input);
        Assert.Equal(SetOperation.UnionAll, union.Operation);
        Assert.Equal(3, union.Inputs.Count);
        Assert.All(union.Inputs, input =>
        {
            var fetch = Assert.IsType<FetchRequiredColumnsRelation>(input);
            Assert.IsType<MergeJoinRelation>(fetch.Input);
            Assert.Equal(8, fetch.OutputLength);
        });
    }
}
