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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Google.Protobuf.Collections;
using Qdrant.Client;
using Qdrant.Client.Grpc;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    public class QdrantSink : ColumnGroupedWriteOperator
    {
        private readonly QdrantSinkOptions _options;
        private readonly WriteRelation _writeRelation;
        private readonly IEmbeddingGenerator _embeddingGenerator;
        private readonly IStringChunker? _chunker;

        private readonly QdrantSinkState _state;

        private int _pkIndex = -1;
        private int _vectorIndex = -1;

        private const string FlowtideMetadataPayloadKey = "flowtide";

        public QdrantSink(QdrantSinkOptions options, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions, IEmbeddingGenerator embeddingGenerator, IStringChunker? chunker) : base(options.ExecutionMode, writeRelation, executionDataflowBlockOptions)
        {
            _options = options;
            _writeRelation = writeRelation;

            _embeddingGenerator = embeddingGenerator;
            _chunker = chunker;

            _state = new QdrantSinkState
            {
                CollectionName = _options.CollectionName,
                OriginalCollectionName = _options.CollectionName,
                ExtraData = []
            };
        }

        public override string DisplayName => "QdrantSink";

        protected override void Checkpoint(long checkpointTime)
        {
            _state.LastCheckpointTime = DateTimeOffset.UtcNow;
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            _pkIndex = _writeRelation.TableSchema.Names.FindIndex(x => x.Equals(_options.IdColumnName, StringComparison.OrdinalIgnoreCase));
            _vectorIndex = _writeRelation.TableSchema.Names.FindIndex(x => x.Equals(_options.VectorStringColumnName, StringComparison.OrdinalIgnoreCase));

            return ValueTask.FromResult<IReadOnlyList<int>>([_pkIndex]);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await base.InitializeOrRestore(restoreTime, stateManagerClient);

            _state.StreamVersion = StreamVersion;
            if (_options.OnInitialize != null)
            {
                var grpcClient = new QdrantGrpcClient(_options.QdrantChannelFunc());
                var client = new QdrantClient(grpcClient);
                await _options.OnInitialize(_state, client);
                client.Dispose();
            }
        }

        protected override async Task OnInitialDataSent()
        {
            if (_options.OnInitialDataSent != null)
            {
                var grpcClient = new QdrantGrpcClient(_options.QdrantChannelFunc());
                var client = new QdrantClient(grpcClient);
                await _options.OnInitialDataSent(_state, client);
                client.Dispose();
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            Debug.Assert(_pkIndex != -1);
            Debug.Assert(_vectorIndex != -1);

            Logger.UploadChangesStarted(StreamName);

            ArgumentException.ThrowIfNullOrWhiteSpace(_state.CollectionName);

            await HandleChanges(rows, cancellationToken);

            _state.LastUpdate = DateTimeOffset.UtcNow;
            Logger.UploadChangesDone(StreamName);
        }

        private async Task HandleChanges(IAsyncEnumerable<ColumnWriteOperation> rows, CancellationToken cancellationToken)
        {
            var grpcClient = new QdrantGrpcClient(_options.QdrantChannelFunc());
            var client = new QdrantClient(grpcClient);

            var uuidPointsToDelete = new List<Guid>();
            var numPointsToDelete = new List<ulong>();

            var upsertOperation = new PointsUpdateOperation
            {
                Upsert = new PointsUpdateOperation.Types.PointStructList
                {

                }
            };
            var updateVectorOperation = new PointsUpdateOperation
            {
                UpdateVectors = new PointsUpdateOperation.Types.UpdateVectors
                {
                    Points = { }
                }
            };

            var pointOperations = new List<PointsUpdateOperation>();

            await foreach (var row in rows)
            {
                var resourceId = row.EventBatchData.Columns[_pkIndex].GetValueAt(row.Index, default);
                var resourceKey = $"__{FlowtideMetadataPayloadKey}_id:{resourceId}";
                var points = await ScrollPoints(client, resourceKey, cancellationToken);

                if (row.IsDeleted)
                {
                    uuidPointsToDelete.AddRange(points.Where(s => s.Id.HasUuid).Select(p => Guid.Parse(p.Id.Uuid)));
                    numPointsToDelete.AddRange(points.Where(s => s.Id.HasNum).Select(p => p.Id.Num));
                }
                else
                {
                    var vectorString = row.EventBatchData.Columns[_vectorIndex].GetValueAt(row.Index, default).ToString()
                        ?? throw new InvalidOperationException($"Vector string is null for row {row.Index}");

                    IReadOnlyList<string> chunks;
                    if (_chunker != null)
                    {
                        chunks = await _chunker.Chunk(vectorString, cancellationToken);
                    }
                    else
                    {
                        chunks = [vectorString];
                    }

                    var payloadData = new Struct
                    {
                        Fields =
                        {
                            { "embedding_generator", _embeddingGenerator.GetType().FullName ?? _embeddingGenerator.GetType().Name },
                            { "last_update", DateTimeOffset.UtcNow.ToString() },
                        }
                    };

                    var basePoint = new PointStruct();
                    for (int i = 0; i < row.EventBatchData.Columns.Count; i++)
                    {
                        if (i != _pkIndex && i != _vectorIndex)
                        {
                            var dataValue = row.EventBatchData.Columns[i].GetValueAt(row.Index, default);
                            var value = GetValue(dataValue);

                            if ((dataValue.Type == ArrowTypeId.Map || dataValue.Type == ArrowTypeId.Struct) && _options.QdrantStoreMapsUnderOwnKey)
                            {
                                basePoint.Payload.Add(_writeRelation.TableSchema.Names[i], value);
                            }
                            else if (dataValue.Type == ArrowTypeId.List && _options.QdrantStoreListsUnderOwnKey)
                            {
                                basePoint.Payload.Add(_writeRelation.TableSchema.Names[i], value);
                            }
                            else
                            {
                                payloadData.Fields.Add(_writeRelation.TableSchema.Names[i], value);
                            }
                        }
                    }

                    var chunkIndex = 0;
                    foreach (var chunk in chunks)
                    {
                        PointId pointId;
                        bool updateVector = true;
                        bool isNewPoint = true;
                        if (points.Count > chunkIndex)
                        {
                            isNewPoint = false;
                            var existingPoint = points[chunkIndex];
                            pointId = existingPoint.Id;
                            var a = existingPoint.Payload.FirstOrDefault(s => s.Key == _options.QdrantPayloadDataPropertyName);

                            if (a.Value.StructValue != null)
                            {
                                var textField = a.Value.StructValue.Fields.FirstOrDefault(s => s.Key == _options.QdrantVectorTextPropertyName);
                                updateVector = textField.Value.StringValue != chunk;
                            }
                        }
                        else
                        {
                            pointId = Guid.NewGuid();
                        }

                        var newPoint = new PointStruct(basePoint)
                        {
                            Id = pointId
                        };

                        var chunkPayload = new Struct(payloadData);
                        if (_options.QdrantIncludeVectorTextInPayload)
                        {
                            chunkPayload.Fields.Add(_options.QdrantVectorTextPropertyName, chunk);
                        }

                        newPoint.Payload.Add(_options.QdrantPayloadDataPropertyName, new Value
                        {
                            StructValue = chunkPayload
                        });

                        var flowtidePayload = new List<string>
                        {
                            $"__{FlowtideMetadataPayloadKey}_id:{resourceId}",
                        };

                        if (_chunker != null)
                        {
                            flowtidePayload.Add($"__{FlowtideMetadataPayloadKey}_chunk:{chunkIndex}");
                        }

                        if (!string.IsNullOrWhiteSpace(StreamVersion?.Version))
                        {
                            flowtidePayload.Add($"__{FlowtideMetadataPayloadKey}_version:{StreamVersion.Version}");
                        }

                        newPoint.Payload.Add(FlowtideMetadataPayloadKey, flowtidePayload.ToArray());

                        // point is new
                        if (isNewPoint)
                        {
                            if (!updateVector)
                            {
                                throw new InvalidOperationException("Point is new but update vector is false.");
                            }

                            newPoint.Vectors = await _embeddingGenerator.GenerateEmbeddingAsync(chunk, cancellationToken);
                            upsertOperation.Upsert.Points.Add(newPoint);
                        }
                        // point is an existing point so update
                        else
                        {
                            // each chunk gets its own operation as they contain chunk metadata information that are unique per chunk

                            if (updateVector)
                            {
                                newPoint.Vectors = await _embeddingGenerator.GenerateEmbeddingAsync(chunk, cancellationToken);
                                updateVectorOperation.UpdateVectors.Points.Add(new PointVectors
                                {
                                    Id = pointId,
                                    Vectors = newPoint.Vectors
                                });
                            }

                            var operation = new PointsUpdateOperation();

                            if (_options.QdrantPayloadUpdateMode == QdrantPayloadUpdateMode.SetPayload)
                            {
                                operation.SetPayload = new PointsUpdateOperation.Types.SetPayload
                                {
                                    PointsSelector = new PointsSelector
                                    {
                                        Points = new PointsIdsList
                                        {
                                            Ids = { pointId }
                                        }
                                    },
                                    Payload = { newPoint.Payload }
                                };
                            }
                            else
                            {
                                operation.OverwritePayload = new PointsUpdateOperation.Types.OverwritePayload
                                {
                                    PointsSelector = new PointsSelector
                                    {
                                        Points = new PointsIdsList
                                        {
                                            Ids = { pointId }
                                        }
                                    },
                                    Payload = { newPoint.Payload }
                                };
                            }

                            pointOperations.Add(operation);
                        }

                        chunkIndex++;
                    }

                    // if there's old points that are not in the new points, delete them
                    if (points.Count > chunks.Count)
                    {
                        uuidPointsToDelete.AddRange(points.Skip(chunks.Count).Where(s => s.Id.HasUuid).Select(p => Guid.Parse(p.Id.Uuid)));
                        numPointsToDelete.AddRange(points.Skip(chunks.Count).Where(s => s.Id.HasNum).Select(p => p.Id.Num));
                    }
                }

                if (upsertOperation.Upsert.Points.Count >= _options.MaxNumberOfBatchOperations)
                {
                    pointOperations.Add(upsertOperation);
                    await HandleAndClearOperations(client, pointOperations, cancellationToken);
                    upsertOperation = new PointsUpdateOperation
                    {
                        Upsert = new PointsUpdateOperation.Types.PointStructList
                        {
                        }
                    };
                }
                else if (updateVectorOperation.UpdateVectors.Points.Count >= _options.MaxNumberOfBatchOperations)
                {
                    pointOperations.Add(updateVectorOperation);
                    await HandleAndClearOperations(client, pointOperations, cancellationToken);
                    updateVectorOperation = new PointsUpdateOperation
                    {
                        UpdateVectors = new PointsUpdateOperation.Types.UpdateVectors
                        {
                            Points = { }
                        }
                    };
                }
                else if (pointOperations.Count >= _options.MaxNumberOfBatchOperations)
                {
                    await HandleAndClearOperations(client, pointOperations, cancellationToken);
                }
            }

            if (upsertOperation.Upsert.Points.Count > 0)
            {
                pointOperations.Add(upsertOperation);
            }

            if (updateVectorOperation.UpdateVectors.Points.Count > 0)
            {
                pointOperations.Add(updateVectorOperation);
            }

            await HandleAndClearOperations(client, pointOperations, cancellationToken);
            await HandleAndClearDeletes(client, uuidPointsToDelete, numPointsToDelete, cancellationToken);

            if (_options.OnChangesDone != null)
            {
                await _options.OnChangesDone(_state, client);
            }

            client.Dispose();
        }

        private async Task HandleAndClearDeletes(QdrantClient client, List<Guid> uuidPointsToDelete, List<ulong> numPointsToDelete, CancellationToken cancellationToken)
        {
            if (uuidPointsToDelete.Count > 0)
            {
                Logger.DeletingPoints(uuidPointsToDelete.Count, StreamName);
                var result = await client.DeleteAsync(_state.CollectionName, uuidPointsToDelete, wait: _options.Wait, cancellationToken: cancellationToken)
                    .ExecutePipeline(_options.ResiliencePipeline);

                if (result.Status != UpdateStatus.Completed && result.Status != UpdateStatus.Acknowledged)
                {
                    throw new InvalidOperationException($"Failed to delete points: {result.Status} on operation {result.OperationId}");
                }
            }

            if (numPointsToDelete.Count > 0)
            {
                Logger.DeletingPoints(numPointsToDelete.Count, StreamName);
                var result = await client.DeleteAsync(_state.CollectionName, numPointsToDelete, wait: _options.Wait, cancellationToken: cancellationToken)
                    .ExecutePipeline(_options.ResiliencePipeline);

                if (result.Status != UpdateStatus.Completed && result.Status != UpdateStatus.Acknowledged)
                {
                    throw new InvalidOperationException($"Failed to delete points: {result.Status} on operation {result.OperationId}");
                }
            }

            uuidPointsToDelete.Clear();
            numPointsToDelete.Clear();
        }

        private async Task HandleAndClearOperations(QdrantClient client, List<PointsUpdateOperation> pointOperations, CancellationToken cancellationToken)
        {
            if (pointOperations.Count > 0)
            {
                Logger.HandlingPoints(pointOperations.Count, StreamName);

                var result = await client.UpdateBatchAsync(_state.CollectionName, pointOperations, wait: _options.Wait, cancellationToken: cancellationToken).ExecutePipeline(_options.ResiliencePipeline);

                var exceptions = new List<Exception>();
                foreach (var item in result)
                {
                    if (item.Status != UpdateStatus.Completed && item.Status != UpdateStatus.Acknowledged)
                    {
                        exceptions.Add(new InvalidOperationException($"Operation {item.OperationId} failed: {item.Status}"));
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }
            }

            foreach (var op in pointOperations)
            {
                op.ClearOperation();
            }

            pointOperations.Clear();
        }

        private async Task<RepeatedField<RetrievedPoint>> ScrollPoints(QdrantClient client, string payloadIdKey, CancellationToken cancellationToken)
        {
            var scrollFilter = new Filter
            {
                Must =
                {
                    new Condition
                    {
                        Field = new FieldCondition
                        {
                            Key = FlowtideMetadataPayloadKey,
                            Match = new Match
                            {
                                Keyword = payloadIdKey
                            }
                        },
                    }
                },
            };

            var searchResult = await client.ScrollAsync(
                _state.CollectionName,
                filter: scrollFilter,
                limit: 1000, // todo: this might not be enough for very large payloads, or with very small chunks
                cancellationToken: cancellationToken);

            return searchResult.Result;
        }

        private static Value GetValue(IDataValue value, int depth = 0)
        {
            if (depth > 5)
            {
                throw new InvalidOperationException("Recursion depth exceeded");
            }

            switch (value.Type)
            {
                case ArrowTypeId.Null:
                    return new Value() { NullValue = new NullValue() };
                case ArrowTypeId.Boolean:
                    return new Value() { BoolValue = value.AsBool };
                case ArrowTypeId.Int64:
                    return new Value() { IntegerValue = value.AsLong };
                case ArrowTypeId.HalfFloat:
                case ArrowTypeId.Float:
                case ArrowTypeId.Double:
                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Decimal256:
                    return new Value() { DoubleValue = value.AsDouble };
                case ArrowTypeId.String:
                case ArrowTypeId.Date32:
                case ArrowTypeId.Date64:
                case ArrowTypeId.Timestamp:
                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                    return value.ToString() ?? "";
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.Map:
                    {
                        var mapVal = value.AsMap;
                        var length = mapVal.GetLength();

                        var s = new Struct();
                        for (int i = 0; i < length; i++)
                        {
                            var key = mapVal.GetKeyAt(i);
                            var val = mapVal.GetValueAt(i);
                            s.Fields.Add(key.ToString(), GetValue(val, depth++));
                        }

                        return new Value() { StructValue = s };
                    }
                case ArrowTypeId.Struct:
                    {
                        var structVal = value.AsStruct;

                        var s = new Struct();
                        for (int i = 0; i < structVal.Header.Count; i++)
                        {
                            var key = structVal.Header.GetColumnName(i);
                            s.Fields.Add(key, GetValue(structVal.GetAt(i), depth++));
                        }

                        return new Value() { StructValue = s };
                    }
                case ArrowTypeId.List:
                    {
                        var listVal = value.AsList;
                        var list = new ListValue();
                        for (int i = 0; i < listVal.Count; i++)
                        {
                            list.Values.Add(GetValue(listVal.GetAt(i)));
                        }

                        return new Value { ListValue = list };
                    }
                case ArrowTypeId.UInt8:
                case ArrowTypeId.Int8:
                case ArrowTypeId.UInt16:
                case ArrowTypeId.Int16:
                case ArrowTypeId.UInt32:
                case ArrowTypeId.Int32:
                case ArrowTypeId.UInt64:
                case ArrowTypeId.Union:
                case ArrowTypeId.Interval:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.Binary:
                case ArrowTypeId.FixedSizeList:
                case ArrowTypeId.Duration:
                case ArrowTypeId.RecordBatch:
                case ArrowTypeId.BinaryView:
                case ArrowTypeId.StringView:
                case ArrowTypeId.ListView:
                default:
                    throw new NotImplementedException($"Value of type '{value.Type}' is not supported");
            }
        }
    }
}