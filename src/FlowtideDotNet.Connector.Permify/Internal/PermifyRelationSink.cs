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
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using Grpc.Core;
using System.Globalization;
using System.Threading.Tasks.Dataflow;
using PermifyProto = Base.V1;

namespace FlowtideDotNet.Connector.Permify.Internal
{
    internal class PermifyRelationSink : SimpleGroupedWriteOperator
    {
        private readonly int m_resourceObjectTypeIndex;
        private readonly int m_resourceObjectIdIndex;
        private readonly int m_relationIndex;
        private readonly int m_subjectObjectTypeIndex;
        private readonly int m_subjectObjectIdIndex;
        private readonly int m_subjectRelationIndex;
        private readonly PermifySinkOptions _permifySinkOptions;
        private readonly PermifyProto.Data.DataClient _dataClient;
        private readonly List<int> m_primaryKeys;

        public PermifyRelationSink(
            WriteRelation writeRelation,
            PermifySinkOptions permifySinkOptions,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions)
            : base(permifySinkOptions.ExecutionMode, executionDataflowBlockOptions)
        {
            _permifySinkOptions = permifySinkOptions;
            _dataClient = new PermifyProto.Data.DataClient(permifySinkOptions.Channel);

            m_resourceObjectTypeIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("entity_type", StringComparison.OrdinalIgnoreCase));
            if (m_resourceObjectTypeIndex == -1)
            {
                throw new InvalidOperationException("Permify sink requires entity_type column");
            }
            m_resourceObjectIdIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("entity_id", StringComparison.OrdinalIgnoreCase));
            if (m_resourceObjectIdIndex == -1)
            {
                throw new InvalidOperationException("Permify sink requires entity_id column");
            }
            m_relationIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("relation", StringComparison.OrdinalIgnoreCase));
            if (m_relationIndex == -1)
            {
                throw new InvalidOperationException("Permify sink requires relation column");
            }
            m_subjectObjectTypeIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("subject_type", StringComparison.OrdinalIgnoreCase));
            if (m_subjectObjectTypeIndex == -1)
            {
                throw new InvalidOperationException("Permify sink requires subject_type column");
            }
            m_subjectObjectIdIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("subject_id", StringComparison.OrdinalIgnoreCase));
            if (m_subjectObjectIdIndex == -1)
            {
                throw new InvalidOperationException("Permify sink requires subject_id column");
            }
            m_subjectRelationIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("subject_relation", StringComparison.OrdinalIgnoreCase));

            m_primaryKeys = new List<int>() { m_resourceObjectTypeIndex, m_resourceObjectIdIndex, m_relationIndex, m_subjectObjectTypeIndex, m_subjectObjectIdIndex };
        }

        public override string DisplayName => "Permify";

        protected override Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            return Task.FromResult(new MetadataResult(m_primaryKeys));
        }

        private static string ColumnToString(scoped in FlxValueRef flxValue)
        {
            switch (flxValue.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    return flxValue.AsBool.ToString();
                case FlexBuffers.Type.Null:
                    return "null";
                case FlexBuffers.Type.Decimal:
                    return flxValue.AsDecimal.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.Float:
                    return flxValue.AsDouble.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.Map:
                    return flxValue.AsMap.ToJson;
                case FlexBuffers.Type.Int:
                    return flxValue.AsLong.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.String:
                    return flxValue.AsString;
                default:
                    throw new InvalidOperationException($"Unsupported type {flxValue.ValueType}");
            }
        }

        private PermifyProto.Tuple RowToTuple(in SimpleChangeEvent row)
        {
            var resourceObjectType = row.Row.GetColumnRef(m_resourceObjectTypeIndex);
            var resourceObjectId = row.Row.GetColumnRef(m_resourceObjectIdIndex);
            var relation = row.Row.GetColumnRef(m_relationIndex);
            var subjectObjectType = row.Row.GetColumnRef(m_subjectObjectTypeIndex);
            var subjectObjectId = row.Row.GetColumnRef(m_subjectObjectIdIndex);

            string? subjectOptionalRelation = null;
            if (m_subjectRelationIndex >= 0)
            {
                subjectOptionalRelation = ColumnToString(row.Row.GetColumnRef(m_subjectRelationIndex));
            }

            var subject = new PermifyProto.Subject()
            {
                Id = ColumnToString(subjectObjectId),
                Type = ColumnToString(subjectObjectType)
            };

            if (subjectOptionalRelation != null)
            {
                subject.Relation = subjectOptionalRelation;
            }

            return new PermifyProto.Tuple()
            {
                Entity = new PermifyProto.Entity()
                {
                    Id = ColumnToString(resourceObjectId),
                    Type = ColumnToString(resourceObjectType),
                },
                Relation = ColumnToString(relation),
                Subject = subject
            };
        }

        private string GetDeleteKey(PermifyProto.Tuple tuple)
        {
            var key = tuple.Entity.Type + ":" + tuple.Relation + ":" + tuple.Subject.Type;
            if (!string.IsNullOrEmpty(tuple.Subject.Relation))
            {
                key += ":" + tuple.Subject.Relation;
            }
            return key;
        }

        private PermifyProto.RelationshipWriteRequest NewWriteRequest()
        {
            return new PermifyProto.RelationshipWriteRequest()
            {
                TenantId = _permifySinkOptions.TenantId,
                Metadata = new PermifyProto.RelationshipWriteRequestMetadata()
            };
        }

        private Metadata GetMetadata()
        {
            if (_permifySinkOptions.GetMetadata != null)
            {
                return _permifySinkOptions.GetMetadata();
            }
            return new Metadata();
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            var writeRequest = NewWriteRequest();

            Dictionary<string, DeleteRequest> deleteObjects = new Dictionary<string, DeleteRequest>();
            int deleteCounter = 0;
            string? lastToken = default;

            await foreach (var row in rows)
            {
                var tuple = RowToTuple(row);

                if (row.IsDeleted)
                {
                    var deleteKey = GetDeleteKey(tuple);
                    if (!deleteObjects.TryGetValue(deleteKey, out var deleteRequest))
                    {
                        deleteRequest = new DeleteRequest(tuple.Entity.Type, tuple.Relation, tuple.Subject.Type, tuple.Subject.Relation);
                        deleteObjects.Add(deleteKey, deleteRequest);
                    }
                    deleteRequest.Add(tuple);
                    deleteCounter++;
                }
                else
                {
                    writeRequest.Tuples.Add(tuple);
                }

                if (writeRequest.Tuples.Count >= _permifySinkOptions.BatchSize)
                {
                    var writeResponse = await _dataClient.WriteRelationshipsAsync(writeRequest, GetMetadata());
                    lastToken = writeResponse.SnapToken;
                    writeRequest = NewWriteRequest();
                }
                if (deleteCounter >= _permifySinkOptions.BatchSize)
                {
                    // Send all delete requests
                    foreach (var deleteRequest in deleteObjects.Values)
                    {
                        var deleteResponse = await _dataClient.DeleteRelationshipsAsync(new PermifyProto.RelationshipDeleteRequest()
                        {
                            TenantId = _permifySinkOptions.TenantId,
                            Filter = deleteRequest.GetFilter()
                        }, GetMetadata());
                        lastToken = deleteResponse.SnapToken;
                    }
                    deleteObjects.Clear();
                    deleteCounter = 0;
                }
            }

            if (writeRequest.Tuples.Count > 0)
            {
                var writeResponse = await _dataClient.WriteRelationshipsAsync(writeRequest, GetMetadata());
                lastToken = writeResponse.SnapToken;
            }

            foreach (var deleteRequest in deleteObjects.Values)
            {
                var deleteResponse = await _dataClient.DeleteRelationshipsAsync(new PermifyProto.RelationshipDeleteRequest()
                {
                    TenantId = _permifySinkOptions.TenantId,
                    Filter = deleteRequest.GetFilter()
                }, GetMetadata());
                lastToken = deleteResponse.SnapToken;
            }

            if (_permifySinkOptions.OnWatermarkFunc != null && lastToken != null)
            {
                await _permifySinkOptions.OnWatermarkFunc(watermark, lastToken);
            }
        }
    }
}
