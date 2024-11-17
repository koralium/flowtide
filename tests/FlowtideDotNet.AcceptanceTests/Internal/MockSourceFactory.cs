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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Substrait.Protobuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockSourceFactory : RegexConnectorSourceFactory
    {
        private readonly MockDatabase mockDatabase;

        public MockSourceFactory(string regexPattern, MockDatabase mockDatabase) : base(regexPattern)
        {
            this.mockDatabase = mockDatabase;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            var table = mockDatabase.GetTable(readRelation.NamedTable.DotSeperated);

            List<int> pks = new List<int>();
            foreach (var primaryKeyIndex in table.PrimaryKeyIndices)
            {
                var pkIndex = readRelation.BaseSchema.Names.IndexOf(table.Columns[primaryKeyIndex]);
                if (pkIndex >= 0)
                {
                    pks.Add(pkIndex);
                }
                else
                {
                    readRelation.BaseSchema.Names.Add(table.Columns[primaryKeyIndex]);
                    if (readRelation.BaseSchema.Struct == null)
                    {
                        readRelation.BaseSchema.Struct = new Struct()
                        {
                            Types = new List<SubstraitBaseType>()
                        };
                    }
                    readRelation.BaseSchema.Struct.Types.Add(new AnyType());
                    pks.Add(readRelation.BaseSchema.Names.Count - 1);
                }
            }

            return new NormalizationRelation()
            {
                Emit = readRelation.Emit,
                Filter = readRelation.Filter,
                Input = readRelation,
                KeyIndex = pks
            };
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new MockDataSourceOperator(readRelation, mockDatabase, dataflowBlockOptions);
        }
    }
}
