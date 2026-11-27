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
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    private static Plan GroupOver(Relation leaf, int columns)
    {
        var second = (MergeJoinRelation)JoinOnColumn(leaf, "d2", 2);
        var third = (MergeJoinRelation)JoinedTo(second, "d3");
        second.MultiJoinGroup = 1;
        third.MultiJoinGroup = 1;
        return Write(third, columns);
    }

    [Fact]
    public void AGroupOverAFilterThatReadsIsAReaderOfItsOwn()
    {
        var first = JoinedTo(Wide("fact", 8, new StringType()), "d1");
        var filter = new FilterRelation() { Input = first, Condition = Compare(FunctionsComparison.Equal, Field(1), Field(1)) };

        var (counted, placed) = ReadersCountedAndPlaced(GroupOver(filter, 12));

        Assert.Equal(placed, counted);
    }

    [Fact]
    public void AGroupOverAProjectThatComputesIsAReaderOfItsOwn()
    {
        var first = JoinedTo(Wide("fact", 8, new StringType()), "d1");
        var project = new ProjectRelation()
        {
            Input = first,
            Expressions = [Compare(FunctionsComparison.Equal, Field(1), Field(1))]
        };

        var (counted, placed) = ReadersCountedAndPlaced(GroupOver(project, 13));

        Assert.Equal(placed, counted);
    }

    [Fact]
    public void ValuesReadAtAJoinSideAreReportedUnderARelationOfThePlan()
    {
        var read = Wide("plain", 8, new StringType());
        var first = new MergeJoinRelation()
        {
            Left = read,
            Right = Wide("d1", 3, new StringType()),
            Type = JoinType.Inner,
            LeftKeys = [Field(1), Field(2), Field(3), Field(4)],
            RightKeys = [Field(9), Field(10), Field(11), Field(12)]
        };
        var plan = Write(JoinedTo(JoinedTo(JoinedTo(first, "d2"), "d3"), "d4"), 16);

        var report = Analyze(plan).report;

        Assert.NotEmpty(report.MidPlan);
        Assert.DoesNotContain(report.MidPlan, x => x.Producer == nameof(ReferenceRelation));
    }

    [Fact]
    public async Task APlanWhoseCopyDecidesOtherwisePlacesNothing()
    {
        var exchange = new ExchangeRelation()
        {
            Input = Wide("fact", 8, new StringType()),
            ExchangeKind = new ScatterExchangeKind() { Fields = [Field(0)] },
            PartitionCount = 0,
            Targets =
            [
                new StandardOutputExchangeTarget() { PartitionIds = [0] },
                new StandardOutputExchangeTarget() { PartitionIds = [1] }
            ]
        };
        var reference = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 9 };
        var plan = Write(JoinedTo(reference, "d1"), 10);
        plan.Relations.Insert(0, exchange);
        Assert.True(Of(Analyze(plan).report, "fact").Admitted);

        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = RunnableBuilder(plan, LateMaterializationMode.Auto)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });
        try
        {
            await using var stream = builder.Build();
        }
        catch (Exception)
        {
        }

        Assert.Contains("reads back differently", builder.LateMaterializationReport!.Skipped);
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
        Assert.False(RowStoreHostCollector.UsesRowStores(plan.Relations));
    }

    private static Plan StaleReference(int length)
    {
        var join = new MergeJoinRelation()
        {
            Left = new NormalizationRelation() { Input = Wide("t1", 6, new StringType()), KeyIndex = [0] },
            Right = new NormalizationRelation() { Input = Wide("t2", 6, new StringType()), KeyIndex = [0] },
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(7)]
        };
        var filter = new FilterRelation()
        {
            Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = length },
            Condition = Compare(FunctionsComparison.Equal, Field(0), Field(0)),
            Emit = [0, 1, 2, 3, 4, 5, 6]
        };
        var plan = Write(filter, 7);
        plan.Relations.Insert(0, join);
        return plan;
    }

    [Theory]
    [InlineData(LateMaterializationMode.Off)]
    [InlineData(LateMaterializationMode.Auto)]
    public void AutoBuildsAPlanWithAStaleReferenceLengthWhereverOffBuildsIt(LateMaterializationMode mode)
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = RunnableBuilder(StaleReference(7), mode)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });

        var error = Record.Exception(() => builder.Build().DisposeAsync().AsTask().GetAwaiter().GetResult());

        Assert.True(error == null, $"{mode}: {error}; {builder.LateMaterializationReport}");
    }
}
