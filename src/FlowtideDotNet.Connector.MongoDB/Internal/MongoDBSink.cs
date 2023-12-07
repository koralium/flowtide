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

using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class MongoDBSink : SimpleGroupedWriteOperator
    {
        private readonly FlowtideMongoDBSinkOptions options;
        private readonly WriteRelation writeRelation;
        private readonly StreamEventToBson streamEventToBson;
        private IMongoCollection<BsonDocument> collection;
        List<int> primaryKeys;

        public MongoDBSink(FlowtideMongoDBSinkOptions options, WriteRelation writeRelation, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, executionDataflowBlockOptions)
        {
            this.options = options;
            this.writeRelation = writeRelation;
            streamEventToBson = new StreamEventToBson(writeRelation.TableSchema.Names);
        }

        public override string DisplayName => "MongoDB Sink";

        protected override Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            var connection = new MongoUrl(options.ConnectionString);
            var client = new MongoClient(connection);
            var database = client.GetDatabase(options.Database);
            collection = database.GetCollection<BsonDocument>(options.Collection);
            primaryKeys = new List<int>();
            foreach (var primaryKey in options.PrimaryKeys) 
            {
                var index = writeRelation.TableSchema.Names.FindIndex(x => x.Equals(primaryKey, StringComparison.OrdinalIgnoreCase));
                if (index < 0) 
                {
                    throw new InvalidOperationException($"Primary key '{primaryKey}' not found in table schema");
                }
                primaryKeys.Add(index);
            } 
            return Task.FromResult(new MetadataResult(primaryKeys));
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, CancellationToken cancellationToken)
        {
            List<WriteModel<BsonDocument>> writes = new List<WriteModel<BsonDocument>>();
            await foreach(var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                FilterDefinition<BsonDocument>[] filters = new FilterDefinition<BsonDocument>[primaryKeys.Count];
                for (int i = 0; i < primaryKeys.Count; i++)
                {
                    var pkname = options.PrimaryKeys[i];
                    var col = row.Row.GetColumn(i);
                    // Need to take the row value into a bson value
                    filters[i] = Builders<BsonDocument>.Filter.Eq(pkname, StreamEventToBson.ToBsonValue(col));
                }
                FilterDefinition<BsonDocument>? filter = null;
                if (filters.Length > 1)
                {
                    filter = Builders<BsonDocument>.Filter.And(filters);
                }
                else
                {
                    filter = filters[0];
                }
                if (row.IsDeleted)
                {
                    writes.Add(new DeleteOneModel<BsonDocument>(filter));
                }
                else
                {
                    var doc = streamEventToBson.ToBson(row.Row);
                    writes.Add(new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true });
                }

                if (writes.Count >= 1000)
                {
                    await collection.BulkWriteAsync(writes);
                    writes.Clear();
                }
            }

            if (writes.Count > 0)
            {
                await collection.BulkWriteAsync(writes);
                writes.Clear();
            }
        }
    }
}
