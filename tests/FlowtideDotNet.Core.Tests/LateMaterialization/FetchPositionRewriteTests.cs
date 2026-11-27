using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class FetchPositionRewriteTests
{
    [Fact]
    public void FieldsInsideArrayLiteralsAreCopiedToTheirFetchedPositions()
    {
        var original = Field(1);
        var literal = new ArrayLiteral() { Expressions = [original] };
        var project = new ProjectRelation()
        {
            Input = new NormalizationRelation() { Input = Read("t", false), KeyIndex = [0] },
            Expressions = [literal],
            Emit = [4]
        };
        var plan = Write(project, 1);
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(1, LateMaterializationPass.Run(plan, null, functions, 1));

        var moved = Assert.IsType<ArrayLiteral>(Assert.Single(project.Expressions));
        var field = Assert.IsType<DirectFieldReference>(Assert.Single(moved.Expressions));
        Assert.Equal(2, Assert.IsType<StructReferenceSegment>(field.ReferenceSegment).Field);
        Assert.Equal(1, Assert.IsType<StructReferenceSegment>(original.ReferenceSegment).Field);
        Assert.NotSame(literal, moved);
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
    }

    [Fact]
    public void AnOuterProjectReferenceForcesPlainInputAndKeepsItsRoot()
    {
        var outer = Field(1);
        outer.Root = new OuterReference() { StepsOut = 1 };
        var project = new ProjectRelation()
        {
            Input = new NormalizationRelation() { Input = Read("t", false), KeyIndex = [0] },
            Expressions = [outer],
            Emit = [4]
        };
        var plan = Write(project, 1);
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(1, LateMaterializationPass.Run(plan, null, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        Assert.IsType<FetchRequiredColumnsRelation>(project.Input);
        Assert.Equal(4, project.Input.OutputLength);
        Assert.Same(outer, Assert.Single(project.Expressions));
        Assert.Equal((uint)1, Assert.IsType<OuterReference>(outer.Root).StepsOut);
    }

    [Fact]
    public void AReaderAboveAnAliasingProjectFetchesBeforeThePartitionMerge()
    {
        var plan = Join(false, JoinType.Inner);
        var write = Assert.IsType<WriteRelation>(plan.Relations[0]);
        var join = Assert.IsType<MergeJoinRelation>(write.Input);
        join.Left = new NormalizationRelation() { Input = join.Left, KeyIndex = [0] };
        join.Right = new NormalizationRelation() { Input = join.Right, KeyIndex = [0] };
        var alias = new ProjectRelation()
        {
            Input = join,
            Expressions = [Field(2)],
            Emit = [0, 4, 8]
        };
        var reader = new ProjectRelation()
        {
            Input = alias,
            Expressions = [Compare(FunctionsComparison.GreaterThan, Field(2), new StringLiteral() { Value = "m" })]
        };
        write.Input = reader;
        write.TableSchema.Names = ["left_key", "right_key", "text", "flag"];
        ParallelizeOptimizer.Optimize(plan, 3);
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        Assert.Equal(2, LateMaterializationPass.Run(plan, null, functions, 1));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        Assert.Same(alias, reader.Input);
        var union = Assert.IsType<SetRelation>(alias.Input);
        Assert.Equal(3, union.Inputs.Count);
        Assert.All(union.Inputs, input => Assert.IsType<FetchRequiredColumnsRelation>(input));
    }
}
