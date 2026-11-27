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
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Fact]
    public void ReviewerCAliasesReadAsWholeRowsNameAProducerThatExistsInThePlan()
    {
        var project = new ProjectRelation()
        {
            Input = Wide("fact", 8, new StringType()),
            Expressions = Enumerable.Range(1, 8).Select(i => (Expression)Field(i)).ToList()
        };
        var join = (MergeJoinRelation)JoinedTo(project, "d0");
        join.PostJoinFilter = new ScalarFunction()
        {
            ExtensionUrn = "extension:test:rows",
            ExtensionName = "is_odd",
            Arguments = [Field(0)]
        };
        join.Emit = new[] { 0 }.Concat(Enumerable.Range(9, 8)).ToList();
        var plan = Write(JoinedTo(JoinedTo(join, "d1"), "d2"), 11);

        var (_, connectors, functions) = Analyze(plan);
        functions.RegisterScalarFunctionWithExpression(
            "extension:test:rows", "is_odd", x => FlexBuffers.FlxValue.FromBytes(FlexBuffers.FlexBuffer.SingleValue(x.AsLong % 2 == 1)));

        var candidate = Assert.Single(LateMaterializationPass.Analyze(plan, connectors, functions, 1).MidPlan);

        Assert.Equal(8, candidate.Columns);
        Assert.Equal(8 * 32 * 2, candidate.Saving);
        Assert.Equal(nameof(ProjectRelation), candidate.Producer);
    }
}
