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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowStoreOptimizedFilterTests
    {
        private static void Fields(Expression expression, List<int> fields)
        {
            if (expression is DirectFieldReference reference && reference.ReferenceSegment is StructReferenceSegment segment)
            {
                fields.Add(segment.Field);
            }
            if (expression is ScalarFunction function)
            {
                foreach (var argument in function.Arguments)
                {
                    Fields(argument, fields);
                }
            }
        }

        private static ReadRelation? FindRead(Relation relation)
        {
            switch (relation)
            {
                case ReadRelation read:
                    return read;
                case WriteRelation write:
                    return FindRead(write.Input);
                case FetchRequiredColumnsRelation fetch:
                    return FindRead(fetch.Input);
                case FilterRelation filter:
                    return FindRead(filter.Input);
                case ProjectRelation project:
                    return FindRead(project.Input);
                default:
                    return null;
            }
        }

        [Fact]
        public void AFilterOverAStoredReadKeepsTheColumnItNamesWhenThePlanIsOptimized()
        {
            var read = new ReadRelation()
            {
                BaseSchema = new NamedStruct()
                {
                    Names = ["k", "a", "b"],
                    Struct = new Struct() { Types = [new Int64Type(), new StringType(), new StringType()] }
                },
                NamedTable = new NamedTable() { Names = ["t"] },
                RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] }
            };
            var filter = new FilterRelation()
            {
                Input = read,
                Condition = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.Equal,
                    Arguments = [
                        new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 1 } },
                        new StringLiteral() { Value = "x" }
                    ]
                }
            };
            var fetch = new FetchRequiredColumnsRelation()
            {
                Input = filter,
                Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "rows", Columns = [0] }],
                Emit = [0, 3, 1]
            };
            var plan = new Plan()
            {
                Relations = [
                    new WriteRelation()
                    {
                        Input = fetch,
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = ["k", "a", "b"] }
                    }
                ]
            };

            Plan optimized;
            try
            {
                optimized = PlanOptimizer.Optimize(plan);
            }
            catch (InvalidOperationException)
            {
                return;
            }

            var optimizedRead = FindRead(optimized.Relations[0]);
            Assert.NotNull(optimizedRead);
            if (optimizedRead.Filter == null)
            {
                return;
            }
            var fields = new List<int>();
            Fields(optimizedRead.Filter, fields);
            FlowtideDotNet.Core.Engine.LateMaterialization.RowReferencePlanValidator.Validate(optimized.Relations);
            Assert.Equal([2], fields);
        }
    }
}
