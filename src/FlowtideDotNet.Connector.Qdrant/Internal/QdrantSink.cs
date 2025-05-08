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
                var grpcClient = new QdrantGrpcClient(_options.Channel);
                var client = new QdrantClient(grpcClient);
                await _options.OnInitialize(_state, client);
                client.Dispose();
            }
        }

        protected override async Task OnInitialDataSent()
        {
            if (_options.OnInitialDataSent != null)
            {
                var grpcClient = new QdrantGrpcClient(_options.Channel);
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
            var grpcClient = new QdrantGrpcClient(_options.Channel);
            var client = new QdrantClient(grpcClient);

            var pointOperations = new List<PointsUpdateOperation>();

            var uuidPointsToDelete = new List<Guid>();
            var numPointsToDelete = new List<ulong>();

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

                            if (dataValue.Type == ArrowTypeId.Map && _options.QdrantStoreMapsUnderOwnKey)
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
                        if (points.Count > chunkIndex)
                        {
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

                        if (updateVector)
                        {
                            newPoint.Vectors = await _embeddingGenerator.GenerateEmbeddingAsync(chunk, cancellationToken);
                            var upsertOp = new PointsUpdateOperation
                            {
                                Upsert = new PointsUpdateOperation.Types.PointStructList()
                            };

                            upsertOp.Upsert.Points.Add(newPoint);
                            pointOperations.Add(upsertOp);

                        }
                        else
                        {
                            var updateOp = new PointsUpdateOperation();
                            updateOp.SetPayload.PointsSelector.Points.Ids.Add(pointId);
                            updateOp.SetPayload.Payload.Add(newPoint.Payload);
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
            }

            // todo: handle errors, retry
            // todo: split points into batches or upsert them earlier if they are above some limit?
            if (pointOperations.Count > 0)
            {
                Logger.HandlingPoints(pointOperations.Count, StreamName);
                await client.UpdateBatchAsync(_state.CollectionName, pointOperations, wait: _options.Wait, cancellationToken: cancellationToken);
            }

            if (uuidPointsToDelete.Count > 0)
            {
                Logger.DeletingPoints(uuidPointsToDelete.Count, StreamName);
                await client.DeleteAsync(_state.CollectionName, uuidPointsToDelete, wait: _options.Wait, cancellationToken: cancellationToken);
            }

            if (numPointsToDelete.Count > 0)
            {
                Logger.DeletingPoints(numPointsToDelete.Count, StreamName);
                await client.DeleteAsync(_state.CollectionName, numPointsToDelete, wait: _options.Wait, cancellationToken: cancellationToken);
            }

            client.Dispose();
        }

        private async Task<RepeatedField<RetrievedPoint>> ScrollPoints(QdrantClient client, string payloadIdKey, CancellationToken cancellationToken)
        {
            var scrollFilter = new Filter
            {
            };

            scrollFilter.Must.Add(new Condition
            {
                Field = new FieldCondition
                {
                    Key = "flowtide",
                    Match = new Match
                    {
                        Keyword = payloadIdKey
                    }
                },
            });

            var searchResult = await client.ScrollAsync(_state.CollectionName, scrollFilter, cancellationToken: cancellationToken);

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
                case ArrowTypeId.UInt8:
                case ArrowTypeId.Int8:
                case ArrowTypeId.UInt16:
                case ArrowTypeId.Int16:
                case ArrowTypeId.UInt32:
                case ArrowTypeId.Int32:
                case ArrowTypeId.UInt64:
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
                case ArrowTypeId.Struct:
                case ArrowTypeId.Map:
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
                case ArrowTypeId.Union:
                case ArrowTypeId.Interval:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.Binary:
                case ArrowTypeId.List:
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