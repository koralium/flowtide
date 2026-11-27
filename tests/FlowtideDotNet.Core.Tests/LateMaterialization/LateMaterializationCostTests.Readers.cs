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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    private static Relation JoinOnColumn(Relation left, string table, int column)
    {
        return new MergeJoinRelation()
        {
            Left = left,
            Right = Wide(table, 0),
            Type = JoinType.Inner,
            LeftKeys = [Field(column)],
            RightKeys = [Field(left.OutputLength)]
        };
    }

    private static (int Counted, int Placed) ReadersCountedAndPlaced(Plan plan)
    {
        var (report, connectors, functions) = Analyze(plan);
        var fact = Of(report, "fact");
        Assert.True(fact.Admitted, report.ToString());
        Assert.Contains(1, fact.Columns);
        Assert.Contains(2, fact.Columns);
        LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        RowStoreHostCollector.Count(plan.Relations, out _, out int placed);
        return (fact.Fetches, placed);
    }

    [Fact]
    public void AJoinOverAFilterThatReadsIsAReaderOfItsOwn()
    {
        var first = JoinedTo(Wide("fact", 8, new StringType()), "d1");
        var filter = new FilterRelation() { Input = first, Condition = Compare(FunctionsComparison.Equal, Field(1), Field(1)) };
        var plan = Write(JoinedTo(JoinOnColumn(filter, "d2", 2), "d3"), 12);

        var (counted, placed) = ReadersCountedAndPlaced(plan);

        Assert.Equal(placed, counted);
    }

    [Fact]
    public void AJoinOverAProjectThatComputesIsAReaderOfItsOwn()
    {
        var first = JoinedTo(Wide("fact", 8, new StringType()), "d1");
        var project = new ProjectRelation()
        {
            Input = first,
            Expressions = [Compare(FunctionsComparison.Equal, Field(1), Field(1))]
        };
        var plan = Write(JoinedTo(JoinOnColumn(project, "d2", 2), "d3"), 13);

        var (counted, placed) = ReadersCountedAndPlaced(plan);

        Assert.Equal(placed, counted);
    }

    [Fact]
    public void AnAliasKeptAndDeferredPaysItsHeapCopy()
    {
        var read = Wide("fact", 5, new StringType());
        read.Emit = [0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5];
        Expression? condition = null;
        foreach (int position in new[] { 1, 3, 5, 7, 9 })
        {
            var comparison = Compare(FunctionsComparison.GreaterThan, Field(position), new StringLiteral() { Value = "a" });
            condition = condition == null ? comparison : new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.And,
                Arguments = [condition, comparison]
            };
        }
        var plan = Write(JoinedTo(new FilterRelation() { Input = read, Condition = condition! }, "d1"), 12);

        var fact = Of(Analyze(plan).report, "fact");

        Assert.Equal([2, 4, 6, 8, 10], fact.Columns);
        Assert.Equal(8 + (8 + 8 + 8) + 5 * 32 + 16, fact.Cost);
    }

    private static ReadRelation FactWithAStruct()
    {
        var read = Wide("fact", 8, new StringType());
        read.BaseSchema.Names.Add("s");
        read.BaseSchema.Struct!.Types.Add(new NamedStruct() { Names = ["a"], Struct = new Struct() { Types = [new Int64Type()] } });
        return read;
    }

    private static Relation FactFilteredByManyKeys()
    {
        var read = Wide("fact", 8, new StringType());
        Expression condition = Compare(FunctionsComparison.Equal, Field(0), new NumericLiteral() { Value = 0 });
        for (int i = 1; i < 120; i++)
        {
            condition = new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.Or,
                Arguments = [condition, Compare(FunctionsComparison.Equal, Field(0), new NumericLiteral() { Value = i })]
            };
        }
        return new FilterRelation() { Input = read, Condition = condition };
    }

    [Fact]
    public async Task AReadWithAStructColumnIsLeftAloneWhileItDoesNotReadBackFromItsSerializedForm()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = RunnableBuilder(Write(FourJoins(FactWithAStruct()), 14), LateMaterializationMode.Auto)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });
        await using var stream = builder.Build();

        Assert.Contains("cannot be copied through its serialized form", builder.LateMaterializationReport!.Skipped);
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
    }

    [Theory]
    [InlineData(false)]
    public async Task AutoPlacesTheStoreItsAnalysisAdmitsWhateverThePlanReadsBackFromText(bool structColumn)
    {
        Plan MakePlan() => structColumn ? Write(FourJoins(FactWithAStruct()), 14) : Write(FourJoins(FactFilteredByManyKeys()), 13);
        var analysed = Of(Analyze(MakePlan()).report, "fact");
        Assert.True(analysed.Admitted, analysed.Reason);

        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = RunnableBuilder(MakePlan(), LateMaterializationMode.Auto)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });
        await using var stream = builder.Build();

        Assert.True(builder.LateMaterializationReport!.Skipped.Length == 0, builder.LateMaterializationReport!.Skipped);
        Assert.Equal(1, builder.LateMaterializationReport!.Admitted);
    }
}
