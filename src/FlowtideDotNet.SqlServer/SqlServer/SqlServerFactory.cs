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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Substrait.Tests.SqlServer
{
    public class SqlServerFactory : IReadWriteFactory
    {
        private readonly string connectionString;

        public SqlServerFactory(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public ReadOperatorInfo GetReadOperator(ReadRelation readRelation, DataflowBlockOptions dataflowBlockOptions)
        {
            int keyIndex = -1;
            for (int i = 0; i < readRelation.BaseSchema.Struct.Types.Count; i++)
            {
                var type = readRelation.BaseSchema.Struct.Types[i];
                if (type.Nullable == false)
                {
                    keyIndex = i;
                }
            }
            if (keyIndex == -1)
            {
                throw new NotSupportedException("One column must be not nullable");
            }

            var sqlDataSource = new SqlServerDataSource(() => connectionString, readRelation, dataflowBlockOptions);
            return new ReadOperatorInfo(sqlDataSource, new NormalizationRelation()
            {
                Filter= readRelation.Filter,
                Input = readRelation,
                KeyIndex = new List<int>() { keyIndex }
            });
        }

        public IStreamEgressVertex GetWriteOperator(WriteRelation readRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            return new SqlServerSink(() => connectionString, readRelation, executionDataflowBlockOptions);
        }
    }
}
