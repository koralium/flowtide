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
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class RowStoreSubqueryOptimizerTests
{
    [Fact]
    public void AStoreInsideAnExistsSubqueryCannotBypassTheOptimizerGuard()
    {
        var hostedRead = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["inner"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "payload", "flag"],
                Struct = new Struct { Types = [new Int64Type(), new StringType(), new StringType()] }
            },
            RowStore = new RowStoreDefinition { Name = "rows", StoredColumns = [1] }
        };
        var fetch = new FetchRequiredColumnsRelation
        {
            Input = new FilterRelation
            {
                Input = hostedRead,
                Condition = new ScalarFunction
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.Equal,
                    Arguments =
                    [
                        new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 1 } },
                        new StringLiteral { Value = "x" }
                    ]
                }
            },
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 2, Columns = [0] }],
            Emit = [0, 3, 1]
        };
        var plan = new Plan
        {
            Relations =
            [
                new FilterRelation
                {
                    Input = new ReadRelation
                    {
                        NamedTable = new NamedTable { Names = ["outer"] },
                        BaseSchema = new NamedStruct
                        {
                            Names = ["key"],
                            Struct = new Struct { Types = [new Int64Type()] }
                        }
                    },
                    Condition = new SetPredicateExpression { Relation = fetch }
                }
            ]
        };

        var exception = Record.Exception(() => PlanOptimizer.Optimize(plan));

        if (exception != null)
        {
            Assert.IsType<InvalidOperationException>(exception);
            Assert.Contains("cannot be optimized", exception.Message);
            return;
        }
        RowReferencePlanValidator.Validate(plan.Relations);
        Assert.Same(hostedRead, RowStoreHostCollector.Collect(plan.Relations)["rows"]);
        var condition = Assert.IsType<ScalarFunction>(hostedRead.Filter);
        var field = Assert.IsType<DirectFieldReference>(condition.Arguments[0]);
        Assert.Equal(2, Assert.IsType<StructReferenceSegment>(field.ReferenceSegment).Field);
    }
}
