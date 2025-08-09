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
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class MongoDbSourceFactory : AbstractConnectorSourceFactory, IConnectorTableProviderFactory
    {
        private readonly FlowtideMongoDbSourceOptions options;
        private readonly MongoDbTableProvider mongoDbTableProvider;

        public MongoDbSourceFactory(FlowtideMongoDbSourceOptions options)
        {
            this.options = options;
            mongoDbTableProvider = new MongoDbTableProvider(options.ConnectionString);
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            return mongoDbTableProvider.TryGetTableInformation(readRelation.NamedTable.Names, out _);
        }

        public ITableProvider Create()
        {
            return mongoDbTableProvider;
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            if (readRelation.NamedTable.Names.Count != 2)
            {
                throw new InvalidOperationException("MongoDB table name must be in the format 'database.collection' but was " + readRelation.NamedTable.DotSeperated);
            }
            var database = readRelation.NamedTable.Names[0];
            var collection = readRelation.NamedTable.Names[1];
            return new MongoDbSource(options, database, collection, readRelation, functionsRegister, dataflowBlockOptions);
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            var idFieldIndex = readRelation.BaseSchema.Names.IndexOf("_id");
            if (idFieldIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("_id");
                if (readRelation.BaseSchema.Struct != null)
                {
                    readRelation.BaseSchema.Struct.Types.Add(new StringType());
                }
            }
            return readRelation;
        }
    }
}
