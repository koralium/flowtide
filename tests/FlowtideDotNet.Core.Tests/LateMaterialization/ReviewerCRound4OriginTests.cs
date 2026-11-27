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

using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Fact]
    public void ReviewerCGroupInputFetchDoesNotMergeWithItsFiltersOwnFetch()
    {
        var first = JoinedTo(Wide("fact", 8, new StringType()), "d1");
        var filter = new FilterRelation() { Input = first, Condition = Compare(FunctionsComparison.Equal, Field(1), Field(1)) };
        var second = (MergeJoinRelation)JoinOnColumn(filter, "d2", 2);
        second.MultiJoinGroup = 1;
        var third = (MergeJoinRelation)JoinedTo(second, "d3");
        third.MultiJoinGroup = 1;
        var plan = Write(third, 12);

        var (counted, placed) = ReadersCountedAndPlaced(plan);

        Assert.Equal(3, placed);
        Assert.Equal(placed, counted);
    }

    [Fact]
    public void ReviewerCPostJoinIntervalNamesAProducerThatExistsInThePlan()
    {
        var read = Wide("fact", 8, new StringType());
        var join = new MergeJoinRelation()
        {
            Left = read,
            Right = Wide("plain_dimension", 8, new StringType()),
            Type = JoinType.Inner,
            LeftKeys = Enumerable.Range(1, 8).Select(i => (FieldReference)Field(i)).ToList(),
            RightKeys = Enumerable.Range(10, 8).Select(i => (FieldReference)Field(i)).ToList(),
            Emit = Enumerable.Range(0, 9).ToList()
        };
        var plan = Write(JoinedTo(JoinedTo(join, "d1"), "d2"), 11);

        var candidate = Assert.Single(Analyze(plan).report.MidPlan);

        Assert.Equal(8, candidate.Columns);
        Assert.Equal(8 * 32 * 2, candidate.Saving);
        Assert.Contains(candidate.Producer, new[] { nameof(ReadRelation), nameof(MergeJoinRelation), nameof(WriteRelation) });
    }
}
