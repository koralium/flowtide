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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
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

        protected override Task OnInitialDataSent()
        {
            if (options.OnInitialDataSent != null)
            {
                return options.OnInitialDataSent(collection);
            }
            else
            {
                return Task.CompletedTask;
            }
        }

        protected override Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            var urlBuilder = new MongoUrlBuilder(options.ConnectionString);
            var connection = urlBuilder.ToMongoUrl();
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

        private Task WriteData(List<WriteModel<BsonDocument>> writes, CancellationToken cancellationToken)
        {
            if (writes.Count > 0)
            {
                return Task.Factory.StartNew((state) =>
                {
                    var w = state as List<WriteModel<BsonDocument>>;
                    return WriteDataTask(w, cancellationToken);
                }, writes)
                    .Unwrap();
            }
            return Task.CompletedTask;
        }

        private async Task WriteDataTask(List<WriteModel<BsonDocument>> writes, CancellationToken cancellationToken)
        {
            if (writes.Count > 0)
            {
                int retryCount = 0;
                while (retryCount < 10)
                {
                    try
                    {
                        await collection.BulkWriteAsync(writes);
                        return;
                    }
                    catch (Exception e)
                    {
                        if (retryCount == 10)
                        {
                            throw;
                        }
                        Logger.LogWarning("Failed to write to mongoDB, will retry");
                        retryCount++;
                        await Task.Delay(TimeSpan.FromSeconds(5));
                    }
                }
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            List<WriteModel<BsonDocument>> writes = new List<WriteModel<BsonDocument>>();
            List<Task> writeTasks = new List<Task>();
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
                    if (options.TransformDocument != null)
                    {
                        options.TransformDocument(doc);
                    }
                    writes.Add(new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true });
                }

                if (writes.Count >= 100)
                {
                    while (writeTasks.Count >= 100)
                    {
                        for(int i = 0; i < writeTasks.Count; i++)
                        {
                            if (writeTasks[i].IsCompleted)
                            {
                                if (writeTasks[i].IsFaulted)
                                {
                                    var exception = writeTasks[i].Exception;
                                    if (exception != null)
                                    {
                                        throw exception;
                                    }
                                    else
                                    {
                                        throw new InvalidOperationException("MongoDB write failed without exception");
                                    }
                                }
                                writeTasks.RemoveAt(i);
                            }
                        }
                        if (writeTasks.Count >= 100)
                        {
                            await Task.WhenAny(writeTasks);
                        }
                    }

                    writeTasks.Add(WriteData(writes, cancellationToken));
                    writes = new List<WriteModel<BsonDocument>>();
                }
            }

            if (writes.Count > 0)
            {
                writeTasks.Add(collection.BulkWriteAsync(writes));
            }

            await Task.WhenAll(writeTasks);

            if (options.OnWatermarkUpdate != null)
            {
                await options.OnWatermarkUpdate(watermark);
            }
        }
    }
}
