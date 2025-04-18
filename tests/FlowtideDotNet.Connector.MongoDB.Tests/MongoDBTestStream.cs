﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Core;
using MongoDB.Bson;
using MongoDB.Driver;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    internal class MongoDBTestStream : FlowtideTestStream
    {
        private readonly MongoDBFixture mongoDBFixture;
        private readonly string databaseName;
        private readonly string collection;
        private readonly List<string> primaryKeys;
        private readonly Action<BsonDocument>? transform;
        private readonly Func<IMongoCollection<BsonDocument>, Task>? onInitialDataSent;
        private readonly bool addSink;
        private readonly bool addSource;
        private readonly bool disableOperationTime;

        public MongoDBTestStream(MongoDBFixture mongoDBFixture,
            string databaseName,
            string collection,
            List<string> primaryKeys,
            string testName,
            Action<BsonDocument>? transform = null,
            Func<IMongoCollection<BsonDocument>, Task>? onInitialDataSent = null,
            bool addSink = false,
            bool addSource = false,
            bool disableOperationTime = false) : base(testName)
        {
            this.mongoDBFixture = mongoDBFixture;
            this.databaseName = databaseName;
            this.collection = collection;
            this.primaryKeys = primaryKeys;
            this.transform = transform;
            this.onInitialDataSent = onInitialDataSent;
            this.addSink = addSink;
            this.addSource = addSource;
            this.disableOperationTime = disableOperationTime;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManger)
        {
            if (addSource)
            {
                connectorManger.AddMongoDbSource(new FlowtideMongoDbSourceOptions()
                {
                    ConnectionString = mongoDBFixture.GetConnectionString(),
                    DisableOperationTime = disableOperationTime,
                    FullReloadIntervalForNonReplicaSets = TimeSpan.FromMilliseconds(50)
                });
            }
            else
            {
                base.AddReadResolvers(connectorManger);
            }
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            if (addSink)
            {
                factory.AddMongoDbSink("*", new FlowtideMongoDBSinkOptions()
                {
                    Collection = collection,
                    Database = databaseName,
                    ConnectionString = mongoDBFixture.GetConnectionString(),
                    PrimaryKeys = primaryKeys,
                    TransformDocument = transform,
                    OnInitialDataSent = onInitialDataSent
                });
            }
            else
            {
                base.AddWriteResolvers(factory);
            }
        }
    }
}
