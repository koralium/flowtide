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
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class RowStoreOptimizerRefusalTests
{
    [Fact]
    public void OptimizingAStoreBearingPlanMustNotReinterpretAKeptColumn()
    {
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "payload", "flag"],
                Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType()] }
            },
            RowStore = new RowStoreDefinition { Name = "rows", StoredColumns = [1] }
        };
        var filter = new FilterRelation
        {
            Input = read,
            Condition = new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 1 } }
        };
        var fetch = new FetchRequiredColumnsRelation
        {
            Input = filter,
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 2, Columns = [0] }],
            Emit = [0, 1, 3]
        };
        var plan = new Plan { Relations = [fetch] };
        RowReferencePlanValidator.Validate(plan.Relations);

        var exception = Record.Exception(() => PlanOptimizer.Optimize(plan));

        if (exception != null)
        {
            Assert.True(exception is InvalidOperationException or NotSupportedException);
            return;
        }
        if (read.Filter == null)
        {
            Assert.Same(filter, fetch.Input);
            return;
        }
        RowReferencePlanValidator.Validate(plan.Relations);
        var reference = Assert.IsType<DirectFieldReference>(read.Filter);
        Assert.Equal(2, Assert.IsType<StructReferenceSegment>(reference.ReferenceSegment).Field);
    }
}
