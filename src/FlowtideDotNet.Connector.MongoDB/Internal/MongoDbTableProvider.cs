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

using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class MongoDbTableProvider : ITableProvider
    {
        private readonly List<string> _tableNames;
        private readonly MongoClient _client;
        public MongoDbTableProvider(string connectionString)
        {
            var urlBuilder = new MongoUrlBuilder(connectionString);
            var connection = urlBuilder.ToMongoUrl();
            _client = new MongoClient(connection);
            var databaseNames = _client.ListDatabaseNames().ToList();

            _tableNames = new List<string>();
            foreach (var dbName in databaseNames)
            {
                var collections = _client.GetDatabase(dbName).ListCollectionNames().ToList();
                foreach (var collection in collections)
                {
                    _tableNames.Add($"{dbName}.{collection}");
                }
            }
        }
        public bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (_tableNames.Contains(tableName))
            {
                tableMetadata = new TableMetadata(tableName, new NamedStruct()
                {
                    Names = new List<string>() { "_id", "_doc" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new StringType(),
                            new MapType(new StringType(), new AnyType())
                        }
                    }
                });
                return true;
            }
            tableMetadata = default;
            return false;
        }
    }
}
