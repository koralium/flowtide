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

using FlowtideDotNet.Core.Compute.Columnar.Functions.CheckFunctions;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests
{
    public class CheckFunctionFinderTests
    {
        private static ReadRelation Read()
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "table1" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "c0" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                }
            };
        }

        private static Plan Write(Relation input)
        {
            return new Plan()
            {
                Relations = new List<Relation>()
                {
                    new WriteRelation()
                    {
                        Input = input,
                        NamedObject = new NamedTable() { Names = new List<string>() { "output" } },
                        TableSchema = new NamedStruct()
                        {
                            Names = new List<string>() { "c0" },
                            Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                        }
                    }
                }
            };
        }

        private static FieldReference Field0()
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } };
        }

        [Fact]
        public void CheckRelationCountsAsCheckUsage()
        {
            var plan = Write(new CheckRelation()
            {
                Input = Read(),
                Checks = new List<CheckDefinition>()
                {
                    new CheckDefinition()
                    {
                        Condition = Field0(),
                        Message = "failed",
                        Tags = new List<CheckTag>(),
                        Guards = new List<CheckGuard>()
                    }
                }
            });

            Assert.True(CheckFunctionFinder.CheckPlan(plan));
        }

        [Fact]
        public void CheckFunctionCountsAsCheckUsage()
        {
            var plan = Write(new FilterRelation()
            {
                Input = Read(),
                Condition = new ScalarFunction()
                {
                    ExtensionUri = FunctionsCheck.Uri,
                    ExtensionName = FunctionsCheck.CheckTrue,
                    Arguments = new List<Expression>() { Field0(), new StringLiteral() { Value = "failed" } }
                }
            });

            Assert.True(CheckFunctionFinder.CheckPlan(plan));
        }

        [Fact]
        public void PlanWithoutChecksHasNoCheckUsage()
        {
            var plan = Write(new FilterRelation()
            {
                Input = Read(),
                Condition = Field0()
            });

            Assert.False(CheckFunctionFinder.CheckPlan(plan));
        }
    }
}
