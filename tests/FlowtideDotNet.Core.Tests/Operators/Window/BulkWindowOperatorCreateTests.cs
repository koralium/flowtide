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

using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators.Window
{
    public class BulkWindowOperatorCreateTests
    {
        private static ConsistentPartitionWindowRelation Relation(List<WindowFunction> functions)
        {
            return new ConsistentPartitionWindowRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                    BaseSchema = new NamedStruct() { Names = new List<string>() { "a" } }
                },
                WindowFunctions = functions,
                PartitionBy = new List<Expression>(),
                OrderBy = new List<SortField>()
            };
        }

        // Without a function the operator emits the stored rows again on every scan
        [Fact]
        public void RelationWithoutWindowFunctionsIsRejected()
        {
            var created = BulkWindowOperator.TryCreate(
                Relation(new List<WindowFunction>()),
                new FunctionsRegister(),
                new ExecutionDataflowBlockOptions(),
                out var op);

            Assert.False(created);
            Assert.Null(op);
        }
    }
}
