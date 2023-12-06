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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.MongoDB.Extensions;
using FlowtideDotNet.Core.Engine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    internal class MongoDBTestStream : FlowtideTestStream
    {
        private readonly MongoDBFixture mongoDBFixture;
        private readonly string databaseName;
        private readonly string collection;
        private readonly List<string> primaryKeys;

        public MongoDBTestStream(MongoDBFixture mongoDBFixture, string databaseName, string collection, List<string> primaryKeys, string testName) : base(testName)
        {
            this.mongoDBFixture = mongoDBFixture;
            this.databaseName = databaseName;
            this.collection = collection;
            this.primaryKeys = primaryKeys;
        }

        protected override void AddWriteResolvers(ReadWriteFactory factory)
        {
            factory.AddMongoDbSink("*", new FlowtideMongoDBSinkOptions()
            {
                Collection = collection,
                Database = databaseName,
                ConnectionString = mongoDBFixture.GetConnectionString(),
                PrimaryKeys = primaryKeys
            });
        }
    }
}
