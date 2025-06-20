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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class MongoDbSourceState
    {
        public string? ResumeToken { get; set; }

        public string? OperationTime { get; set; }
    }

    internal class MongoDbSource : ColumnBatchReadBaseOperator
    {
        private readonly FlowtideMongoDbSourceOptions _options;
        private readonly string _databaseName;
        private readonly string _collectionName;
        private readonly ReadRelation _readRelation;
        private readonly string _displayName;
        private IMongoCollection<BsonDocument>? collection;
        private int _idFieldIndex;
        private readonly BsonDocToColumn[] _bsonDocToColumns;
        private IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? _cursor;
        private IObjectState<MongoDbSourceState>? _state;
        private bool _watchDisabled = false;
        private DateTimeOffset? _lastFullLoad;

        // These variables are in use if the mongodb does not return ClusterTime
        private int _lastWallTime;
        private int _operationCounter;

        public MongoDbSource(
            FlowtideMongoDbSourceOptions sourceOptions,
            string databaseName,
            string collectionName,
            ReadRelation readRelation,
            IFunctionsRegister functionsRegister,
            DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            this._options = sourceOptions;
            this._databaseName = databaseName;
            this._collectionName = collectionName;
            _readRelation = readRelation;
            _displayName = $"MongoDB Source ({databaseName}.{collectionName})";

            _idFieldIndex = readRelation.BaseSchema.Names.IndexOf("_id");
            if (_idFieldIndex < 0)
            {
                throw new NotSupportedException("MongoDB source requires _id field to be selected.");
            }

            _bsonDocToColumns = new BsonDocToColumn[readRelation.BaseSchema.Names.Count];
            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                _bsonDocToColumns[i] = new BsonDocToColumn(readRelation.BaseSchema.Names[i]);
            }
        }

        public override string DisplayName => _displayName;

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<MongoDbSourceState>("state");

            if (_state.Value == null)
            {
                _state.Value = new MongoDbSourceState();
            }

            if (_cursor != null)
            {
                _cursor.Dispose();
                _cursor = null;
            }
            var urlBuilder = new MongoUrlBuilder(_options.ConnectionString);
            var connection = urlBuilder.ToMongoUrl();
            var client = new MongoClient(connection);
            var database = client.GetDatabase(_databaseName);
            collection = database.GetCollection<BsonDocument>(_collectionName);
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        protected override async Task DeltaLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            if (_watchDisabled && _options.EnableFullReloadForNonReplicaSets)
            {
                if (_lastFullLoad == null || (DateTimeOffset.UtcNow - _lastFullLoad) > _options.FullReloadIntervalForNonReplicaSets)
                {
                    await DoFullLoad(output);
                }
                return;
            }
            if (_cursor != null)
            {
                await DoDeltaLoad(output);
            }
            else
            {
                try
                {
                    if (_state.Value.ResumeToken != null)
                    {
                        // If there is a resume token that can be used to continue listening
                        _cursor = await collection.WatchAsync(new ChangeStreamOptions()
                        {
                            ResumeAfter = BsonDocument.Parse(_state.Value.ResumeToken)
                        });
                        await DoDeltaLoad(output);
                    }
                    else if (_state.Value.OperationTime != null && !_options.DisableOperationTime)
                    {
                        // If there is an operation time that can be used to continue listening
                        using JsonReader jsonReader = new JsonReader(_state.Value.OperationTime);
                        var context = BsonDeserializationContext.CreateRoot(jsonReader);
                        var parsedTimestamp = BsonTimestampSerializer.Instance.Deserialize(context);
                        _cursor = await collection.WatchAsync(new ChangeStreamOptions()
                        {
                            StartAtOperationTime = parsedTimestamp
                        });
                        await DoDeltaLoad(output);
                    }
                    else
                    {
                        // no resume token, must do a full load
                        await DoFullLoad(output);
                    }
                }
                catch (MongoCommandException e)
                {
                    Logger.ChangeStreamDisabledUsingFullLoad(e, StreamName, Name);
                    _watchDisabled = true;
                    if ((_lastFullLoad == null || (DateTimeOffset.UtcNow - _lastFullLoad) > _options.FullReloadIntervalForNonReplicaSets) && _options.EnableFullReloadForNonReplicaSets)
                    {
                        await DoFullLoad(output);
                    }
                }
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_cursor != null);

            PrimitiveList<int>? weights = null;
            PrimitiveList<uint>? iterations = null;
            Column[]? columns = default;

            var cancelTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            while (await _cursor.MoveNextAsync(cancelTokenSource.Token))
            {
                cancelTokenSource.Token.ThrowIfCancellationRequested();
                await EnterCheckpointLock();
                if (weights == null)
                {
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                    columns = new Column[_readRelation.BaseSchema.Names.Count];
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i] = Column.Create(MemoryAllocator);
                    }
                }

                BsonTimestamp? timestamp = default;

                foreach (var doc in _cursor.Current)
                {
                    // Get cluster time, or create a cluster time locally if it is not supported by the server
                    // This will be used as the watermark
                    if (doc.ClusterTime == null)
                    {
                        if (doc.WallTime != null)
                        {
                            var docWallTime = (int)new DateTimeOffset(doc.WallTime.Value).ToUnixTimeSeconds();
                            if (docWallTime == _lastWallTime)
                            {
                                _operationCounter++;
                            }
                            else
                            {
                                _lastWallTime = docWallTime;
                                _operationCounter = 0;
                            }
                        }
                        else
                        {
                            var currentTime = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                            if (currentTime == _lastWallTime)
                            {
                                _operationCounter++;
                            }
                            else
                            {
                                _lastWallTime = currentTime;
                                _operationCounter = 0;
                            }
                        }

                        timestamp = new BsonTimestamp(_lastWallTime, _operationCounter);
                    }
                    else
                    {
                        timestamp = doc.ClusterTime;
                    }

                    if (doc.OperationType == ChangeStreamOperationType.Insert || doc.OperationType == ChangeStreamOperationType.Update)
                    {
                        weights.Add(1);
                        iterations!.Add(0);
                        BsonDocumentToColumns(columns!, doc.FullDocument);
                    }
                    else if (doc.OperationType == ChangeStreamOperationType.Delete)
                    {
                        if (!doc.DocumentKey.TryGetValue("_id", out var id))
                        {
                            throw new InvalidOperationException("Document key does not contain _id field.");
                        }

                        weights.Add(-1);
                        iterations!.Add(0);
                        for (int i = 0; i < columns!.Length; i++)
                        {
                            if (i == _idFieldIndex)
                            {
                                columns[i].Add(new StringValue(id.AsObjectId.ToString()));
                            }
                            else
                            {
                                columns[i].Add(NullValue.Instance);
                            }
                        }
                    }
                    else if (doc.OperationType == ChangeStreamOperationType.Drop)
                    {
                        _state.Value.ResumeToken = null;
                        _state.Value.OperationTime = null;
                        break;
                    }
                    _state.Value.ResumeToken = doc.ResumeToken.ToJson();
                }

                if (weights.Count > 0)
                {
                    yield return new DeltaReadEvent(new EventBatchWeighted(weights, iterations!, new EventBatchData(columns!)), new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(timestamp!.Value)));
                    weights = null;
                    iterations = null;
                    columns = null;
                }
                ExitCheckpointLock();
            }
            _cursor = default;
        }

        private void BsonDocumentToColumns(Column[] columns, BsonDocument document)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                _bsonDocToColumns[i].AddToColumn(columns[i], document);
            }
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(collection != null);
            Debug.Assert(_state?.Value != null);

            _lastFullLoad = DateTimeOffset.UtcNow;

            var cancelTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            var isMasterCommand = new BsonDocument { { "hello", 1 } };
            var res = await collection.Database.RunCommandAsync(new BsonDocumentCommand<BsonDocument>(isMasterCommand));


            if (_cursor == null && !_watchDisabled)
            {
                try
                {
                    if (_state.Value.ResumeToken != null)
                    {
                        _cursor = await collection.WatchAsync(new ChangeStreamOptions()
                        {
                            ResumeAfter = BsonDocument.Parse(_state.Value.ResumeToken)
                        });
                    }
                    else if (res.TryGetValue("operationTime", out var operationTime) && operationTime is BsonTimestamp operationTimestamp && !_options.DisableOperationTime)
                    {
                        var startOperationTime = new BsonTimestamp(operationTimestamp.Timestamp, operationTimestamp.Increment + 1);
                        _state.Value.OperationTime = startOperationTime.ToJson();
                        _cursor = await collection.WatchAsync(new ChangeStreamOptions()
                        {
                            // Add 1 to increment since otherwise it will look at the last operation which is already handled when loading the full data
                            StartAtOperationTime = startOperationTime
                        });
                    }
                    else
                    {
                        _cursor = await collection.WatchAsync(new ChangeStreamOptions()
                        {
                        }, cancellationToken);
                    }

                    this.DeltaLoadInterval = TimeSpan.FromMilliseconds(1);
                }
                catch (MongoCommandException e)
                {
                    Logger.ChangeStreamDisabledUsingFullLoad(e, StreamName, Name);
                    _watchDisabled = true;
                }
            }

            if (!this.DeltaLoadInterval.HasValue)
            {
                this.DeltaLoadInterval = _options.FullReloadIntervalForNonReplicaSets;
            }

            if (!res.TryGetValue("localTime", out var timestamp))
            {
                throw new NotSupportedException("MongoDB source requires localTime field to be selected.");
            }

            long serverTime = 0;
            if (timestamp.BsonType == BsonType.Int64)
            {
                serverTime = timestamp.AsInt64;
            }
            else if (timestamp.BsonType == BsonType.DateTime)
            {
                serverTime = new DateTimeOffset(timestamp.ToUniversalTime()).ToUnixTimeMilliseconds();
            }
            else
            {
                throw new NotSupportedException("MongoDB source requires localTime to be either int64 or datetime.");
            }

            var cursor = await collection.FindAsync(Builders<BsonDocument>.Filter.Empty);

            while (await cursor.MoveNextAsync(cancelTokenSource.Token))
            {
                cancelTokenSource.Token.ThrowIfCancellationRequested();

                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                Column[] columns = new Column[_readRelation.BaseSchema.Names.Count];
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = Column.Create(MemoryAllocator);
                }

                foreach (var doc in cursor.Current)
                {
                    weights.Add(1);
                    iterations.Add(0);
                    BsonDocumentToColumns(columns, doc);
                }

                yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), serverTime);
            }
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult(new List<int>() { _idFieldIndex });
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _readRelation.NamedTable.DotSeperated });
        }
    }
}
