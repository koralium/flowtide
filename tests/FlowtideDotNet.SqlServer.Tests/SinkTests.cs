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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.SqlServer.Tests
{
    /// <summary>
    /// Most sink tests are done in acceptance tests, tests simple functions in sink
    /// </summary>
    public class SinkTests
    {
        [Fact]
        public void TempTableIsTemporary()
        {
            var sink = new SqlServerSink(() => "", new Substrait.Relations.WriteRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new Substrait.Type.NamedTable()
                    {
                        Names = new List<string>() { "table1" }
                    },
                    BaseSchema = new Substrait.Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" }
                    }
                },
                NamedObject = new Substrait.Type.NamedTable(),
                TableSchema = new Substrait.Type.NamedStruct()
            }, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());
            var tableName = sink.GetTmpTableName();
            Assert.StartsWith("#", tableName);
        }
    }
}
