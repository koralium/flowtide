using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    class MongoDBSink : ColumnGroupedWriteOperator
    {
        private readonly FlowtideMongoDBSinkOptions m_options;
        private readonly ColumnsToBsonDoc m_serializer;
        private readonly List<int> primaryKeys;
        private IMongoCollection<BsonDocument>? collection;
        private ICounter<long>? _eventsCounter;

        public MongoDBSink(
            FlowtideMongoDBSinkOptions options,
            ExecutionMode executionMode, 
            WriteRelation writeRelation, 
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            m_options = options;
            m_serializer = new ColumnsToBsonDoc(writeRelation.TableSchema.Names);
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
        }

        public override string DisplayName => "MongoDB Sink";

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override Task OnInitialDataSent()
        {
            Debug.Assert(collection != null);
            if (m_options.OnInitialDataSent != null)
            {
                return m_options.OnInitialDataSent(collection);
            }
            else
            {
                return Task.CompletedTask;
            }
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
            var urlBuilder = new MongoUrlBuilder(m_options.ConnectionString);
            var connection = urlBuilder.ToMongoUrl();
            var client = new MongoClient(connection);

            var database = client.GetDatabase(m_options.Database);
            collection = database.GetCollection<BsonDocument>(m_options.Collection);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return new ValueTask<IReadOnlyList<int>>(primaryKeys);
        }

        private Task WriteData(List<WriteModel<BsonDocument>> writes, CancellationToken cancellationToken)
        {
            if (writes.Count > 0)
            {
                return Task.Factory.StartNew((state) =>
                {
                    var w = (state as List<WriteModel<BsonDocument>>)!;
                    return WriteDataTask(w, cancellationToken);
                }, writes)
                    .Unwrap();
            }
            return Task.CompletedTask;
        }

        private async Task WriteDataTask(List<WriteModel<BsonDocument>> writes, CancellationToken cancellationToken)
        {
            Debug.Assert(writes != null);
            Debug.Assert(collection != null);
            if (writes.Count > 0)
            {
                int retryCount = 0;
                while (retryCount < 10)
                {
                    try
                    {
                        await collection.BulkWriteAsync(writes, cancellationToken: cancellationToken);
                        return;
                    }
                    catch (Exception e)
                    {
                        if (retryCount == 10)
                        {
                            throw;
                        }
                        Logger.FailedToWriteMongoDB(e, StreamName, Name);
                        retryCount++;
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    }
                }
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(collection != null);
            Debug.Assert(_eventsCounter != null);

            List<WriteModel<BsonDocument>> writes = new List<WriteModel<BsonDocument>>();
            List<Task> writeTasks = new List<Task>();
            await foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                FilterDefinition<BsonDocument>[] filters = new FilterDefinition<BsonDocument>[primaryKeys.Count];
                for (int i = 0; i < primaryKeys.Count; i++)
                {
                    var pkname = m_options.PrimaryKeys[i];
                    // Need to take the row value into a bson value
                    filters[i] = Builders<BsonDocument>.Filter.Eq(pkname, m_serializer.ColumnIndexToBson(row.EventBatchData.Columns[primaryKeys[i]], row.Index));
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
                    var doc = m_serializer.ToBson(row.EventBatchData, row.Index);
                    if (m_options.TransformDocument != null)
                    {
                        m_options.TransformDocument(doc);
                    }
                    writes.Add(new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true });
                }

                if (writes.Count >= m_options.DocumentsPerBatch)
                {
                    while (writeTasks.Count >= m_options.ParallelBatches)
                    {
                        for (int i = 0; i < writeTasks.Count; i++)
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
                        if (writeTasks.Count >= m_options.ParallelBatches)
                        {
                            await Task.WhenAny(writeTasks);
                        }
                    }
                    _eventsCounter.Add(writes.Count);
                    writeTasks.Add(WriteData(writes, cancellationToken));
                    writes = new List<WriteModel<BsonDocument>>();
                }
            }

            if (writes.Count > 0)
            {
                _eventsCounter.Add(writes.Count);
                writeTasks.Add(collection.BulkWriteAsync(writes, cancellationToken: cancellationToken));
            }

            await Task.WhenAll(writeTasks);

            if (m_options.OnWatermarkUpdate != null)
            {
                await m_options.OnWatermarkUpdate(watermark);
            }
        }
    }
}
