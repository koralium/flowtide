using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBWholePlanTests
{
    private static Plan RepeatedProjection()
    {
        var normalization = new NormalizationRelation()
        {
            Input = Read("t", false),
            KeyIndex = [1]
        };
        var key = Field(1);
        var project = new ProjectRelation()
        {
            Input = normalization,
            Expressions = [key, key]
        };
        return Write(project, 6);
    }

    [Fact]
    public async Task SharedProjectionReferenceRetainsItsOriginalColumn()
    {
        await using var pair = DifferentialPair.Planned(RepeatedProjection, () => [Table("t", Row(1, 5, "a", "x"))]);
        await pair.StartAsync();
        await pair.SettleBoth();
        pair.AssertNoFailures();
        Assert.Equal(["1|5|a|x|5|5"], pair.Plain.Result());
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }

    private static Plan ProjectionSharedWithTopN()
    {
        var key = Field(1);
        var project = new ProjectRelation()
        {
            Input = new NormalizationRelation()
            {
                Input = Read("t", false),
                KeyIndex = [1]
            },
            Expressions = [key]
        };
        return Write(new TopNRelation()
        {
            Input = project,
            Count = 1,
            Sorts = [new SortField() { Expression = key, SortDirection = SortDirection.SortDirectionAscNullsFirst }]
        }, 5);
    }

    [Fact]
    public async Task PlainProjectionDoesNotMoveAReferenceHeldByTopN()
    {
        await using var pair = DifferentialPair.Planned(ProjectionSharedWithTopN, () => [Table("t", Row(1, 50, "a", "x"), Row(2, 10, "b", "y"))]);
        await pair.StartAsync();
        await pair.SettleBoth();
        pair.AssertNoFailures();
        Assert.Equal(["2|10|b|y|10"], pair.Plain.Result());
        Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
    }
}
