using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBRound4ProjectDemandTests
{
    private static Plan NestedAliases(int depth, bool normalization = true)
    {
        ReferenceRelation Reference() => new() { RelationId = 0, ReferenceOutputLength = 4 };

        var innerUnion = new SetRelation()
        {
            Operation = SetOperation.UnionAll,
            Inputs = [Reference(), Reference()]
        };
        var union = new SetRelation()
        {
            Operation = SetOperation.UnionAll,
            Inputs = [innerUnion, Reference()],
            Emit = [2, 0, 3, 1]
        };
        Relation aliases = new ProjectRelation()
        {
            Input = union,
            Expressions =
            [
                Field(0),
                Field(0),
                Compare(FunctionsComparison.GreaterThan, Field(1), new NumericLiteral() { Value = 0 })
            ],
            Emit = [4, 1, 5, 6, 2]
        };
        for (int i = 0; i < depth; i++)
        {
            aliases = new ProjectRelation()
            {
                Input = aliases,
                Expressions = [Field(2), Field(0)],
                Emit = [5, 1, 6, 3, 4]
            };
        }
        var reader = new ProjectRelation()
        {
            Input = aliases,
            Expressions = [Compare(FunctionsComparison.GreaterThan, Field(0), new StringLiteral() { Value = "m" })],
            Emit = [1, 0, 2, 5, 3]
        };
        var plan = Write(reader, 5);
        var read = Read("t", false);
        plan.Relations.Insert(0, normalization ? new NormalizationRelation() { Input = read, KeyIndex = [0] } : read);
        return plan;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    public void ReorderedDuplicateAliasesFetchOnlyTextBeforeEveryUnion(int depth)
    {
        var plan = NestedAliases(depth);
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(1, LateMaterializationPass.Run(plan, null, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        Relation relation = Assert.IsType<WriteRelation>(plan.Relations[1]).Input;
        for (int i = 0; i < depth + 2; i++)
        {
            relation = Assert.IsType<ProjectRelation>(relation).Input;
        }
        var outer = Assert.IsType<SetRelation>(relation);
        var inner = Assert.IsType<SetRelation>(outer.Inputs[0]);
        foreach (var input in inner.Inputs.Append(outer.Inputs[1]))
        {
            var fetch = Assert.IsType<FetchRequiredColumnsRelation>(input);
            Assert.Equal(new[] { 1 }, Assert.Single(fetch.Fetches).Columns);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    public async Task ReorderedDuplicateAliasesMatchOffAcrossChangesAndRestore(int depth)
    {
        await using var pair = DifferentialPair.Planned(() => NestedAliases(depth, false), () =>
        [
            Table("t", Row(1, 10, "a", "x"), Row(2, 20, "z", null), Row(3, 30, "m", "h"))
        ]);

        await pair.StartAsync();
        await pair.Expect(Triple("1|a|a|false|true", "2|z|z|true|true", "3|m|m|false|true"));
        Assert.Equal(3, Count(pair.Hosted, "Fetch required columns"));
        Assert.Equal(0, Count(pair.Plain, "Fetch required columns"));

        pair.Change(tables => tables["t"].Upsert(Row(1, 11, "zz", null), Row(4, 40, "b", "discarded")));
        pair.Change(tables => tables["t"].Delete(2));
        await pair.Expect(Triple("1|zz|zz|true|true", "3|m|m|false|true", "4|b|b|false|true"));

        await pair.RestartAsync();
        pair.Change(tables => tables["t"].Upsert(Row(3, 31, "n", "changed")));
        pair.Change(tables => tables["t"].Delete(1));
        await pair.Expect(Triple("3|n|n|true|true", "4|b|b|false|true"));
    }

    private static string[] Triple(params string[] rows)
        => rows.SelectMany(row => Enumerable.Repeat(row, 3)).ToArray();
}
