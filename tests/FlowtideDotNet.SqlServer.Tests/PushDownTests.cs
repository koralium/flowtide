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

using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.SqlServer.Tests
{
    public class PushDownTests
    {
        [Fact]
        public void TestInExpressionPushdown()
        {
            var expr = new SingularOrListExpression()
            {
                Value = new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = 0
                    }
                },
                Options = new List<Expression>()
                {
                    new StringLiteral() { Value = "a" },
                    new NumericLiteral() { Value = 17 }
                }
            };
            var visitor = new SqlServerFilterVisitor(new Substrait.Relations.ReadRelation()
            {
                NamedTable = new Substrait.Type.NamedTable()
                {
                    Names = new List<string>() { "table1" }
                },
                BaseSchema = new Substrait.Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" }
                }
            });

            var result = visitor.Visit(expr, default!)!;
            Assert.Equal("[c1] IN ('a', 17)", result.Content);
        }
    }
}
