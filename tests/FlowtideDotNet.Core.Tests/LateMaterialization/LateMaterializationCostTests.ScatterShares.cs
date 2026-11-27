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

using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Theory]
    [InlineData(8, 0, 1.0)]
    [InlineData(8, 3, 0.375)]
    [InlineData(null, 1, 0.5)]
    public void ScatterTargetShareFollowsItsPartitionIds(int? partitionCount, int assignedPartitions, double expectedCopies)
    {
        var exchange = new ExchangeRelation()
        {
            Input = Wide("fact", 1, new StringType()),
            ExchangeKind = new ScatterExchangeKind() { Fields = [Field(0)] },
            PartitionCount = partitionCount,
            Targets =
            [
                new StandardOutputExchangeTarget() { PartitionIds = Enumerable.Range(0, assignedPartitions).ToList() },
                new StandardOutputExchangeTarget() { PartitionIds = [] }
            ]
        };
        var reference = new StandardOutputExchangeReferenceRelation()
        {
            RelationId = 0,
            TargetId = 0,
            ReferenceOutputLength = 2
        };
        var plan = Write(JoinedTo(reference, "plain_dimension"), 3);
        plan.Relations.Insert(0, exchange);

        var candidate = Of(Analyze(plan).report, "fact");

        Assert.Equal(expectedCopies, candidate.Copies);
        Assert.Equal(32 * expectedCopies, candidate.Saving);
    }

    [Fact]
    public void JoinKeyFetchDoesNotMergeWithTheInputFiltersOwnFetch()
    {
        var read = Wide("fact", 6, new StringType());
        read.BaseSchema.Struct!.Types[6] = new DecimalType();
        var filter = new FilterRelation()
        {
            Input = JoinedTo(read, "d1"),
            Condition = Compare(FunctionsComparison.Equal, Field(1), Field(2))
        };
        var right = Wide("plain_dimension", 4, new StringType());
        right.BaseSchema.Struct!.Types[4] = new DecimalType();
        var join = new MergeJoinRelation()
        {
            Left = filter,
            Right = right,
            Type = JoinType.Inner,
            LeftKeys = [Field(3), Field(4), Field(5), Field(6)],
            RightKeys = [Field(9), Field(10), Field(11), Field(12)]
        };
        var plan = Write(join, join.OutputLength);
        var (report, connectors, functions) = Analyze(plan);
        var candidate = Of(report, "fact");

        Assert.Equal(176, candidate.Saving);
        Assert.Equal([1, 2, 3, 4, 5, 6], candidate.Columns);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1,
            new Dictionary<Relation, List<int>>() { [read] = candidate.Columns }));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.IsType<FetchRequiredColumnsRelation>(filter.Input);
        Assert.IsType<FetchRequiredColumnsRelation>(join.Left);
        RowStoreHostCollector.Count(plan.Relations, out int stores, out int readers);
        Assert.Equal(1, stores);
        Assert.Equal(2, readers);
        Assert.Equal(2, candidate.Fetches);
        Assert.Equal(112, candidate.Net);
        Assert.False(candidate.Admitted);
    }
}
