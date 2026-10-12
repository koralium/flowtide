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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading
{
    /// <summary>
    /// Visitor that parses rows in a checkpoint file and converts them to DeltaAction objects.
    /// </summary>
    internal class CheckpointReadVisitor : 
        IArrowArrayVisitor<StructArray>,
        IArrowArrayVisitor<Int32Array>,
        IArrowArrayVisitor<ListArray>,
        IArrowArrayVisitor<StringArray>,
        IArrowArrayVisitor<MapArray>,
        IArrowArrayVisitor<Int64Array>,
        IArrowArrayVisitor<BooleanArray>
    {
        /// <summary>
        /// Stack to keep track of the current index being processed in nested arrays.
        /// </summary>
        private readonly Stack<int> indexStack = new Stack<int>();

        /// <summary>
        /// Stack to keep track of the current field being processed in nested structures.
        /// </summary>
        private readonly Stack<Field> fieldStack = new Stack<Field>();

        /// <summary>
        /// Holds the result of the current visit operation.
        /// </summary>
        private object? result;

        public CheckpointReadVisitor(Field field, int index)
        {
            indexStack.Push(index);
            fieldStack.Push(field);
        }

        /// <summary>
        /// Points the visitor at another cell so one instance can read a whole file.
        /// </summary>
        public void Reset(Field field, int index)
        {
            indexStack.Clear();
            fieldStack.Clear();
            indexStack.Push(index);
            fieldStack.Push(field);
            result = null;
        }

        public void Visit(StructArray array)
        {
            var field = fieldStack.Peek();

            if (field.Name.Equals("protocol", StringComparison.OrdinalIgnoreCase))
            {
                VisitProtocol(field, array);
            }
            else if (field.Name.Equals("metaData", StringComparison.OrdinalIgnoreCase))
            {
                VisitMetadata(field, array);
            }
            else if(field.Name.Equals("format", StringComparison.OrdinalIgnoreCase))
            {
                VisitFormat(field, array);
            }
            else if (field.Name.Equals("add", StringComparison.OrdinalIgnoreCase))
            {
                VisitAdd(field, array);
            }
            else if (field.Name.Equals("deletionVector", StringComparison.OrdinalIgnoreCase))
            {
                VisitDeletionVector(field, array);
            }
            else if (field.Name.Equals("remove", StringComparison.OrdinalIgnoreCase))
            {
                VisitRemove(field, array);
            }
            else if (field.Name.Equals("txn", StringComparison.OrdinalIgnoreCase))
            {
                VisitTxn(field, array);
            }
            else if (field.Name.Equals("domainMetadata", StringComparison.OrdinalIgnoreCase))
            {
                VisitDomainMetadata(field, array);
            }
            else
            {
                throw new NotImplementedException($"Struct field '{field.Name}' not implemented in CheckpointReadVisitor.");
            }
        }

        private void VisitAdd(Field field, StructArray array)
        {
            var index = indexStack.Peek();
            DeltaAddAction addAction = new DeltaAddAction();
            var structType = (field.DataType as StructType)!;
            long? parsedRecordCount = null;

            for (int c = 0; c < structType.Fields.Count; c++)
            {
                fieldStack.Push(structType.Fields[c]);

                if (structType.Fields[c].Name.Equals("path", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    addAction.Path = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("partitionValues", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    addAction.PartitionValues = (Dictionary<string, string>?)result;
                }
                else if (structType.Fields[c].Name.Equals("size", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    
                    if (result != null)
                    {
                        addAction.Size = System.Convert.ToInt64(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("modificationTime", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);

                    if (result != null)
                    {
                        addAction.ModificationTime = System.Convert.ToInt64(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("dataChange", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);

                    if (result != null)
                    {
                        addAction.DataChange = System.Convert.ToBoolean(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("tags", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    addAction.Tags = (Dictionary<string, string>?)result;
                }
                else if (structType.Fields[c].Name.Equals("deletionVector", StringComparison.OrdinalIgnoreCase))
                {
                    if (!array.Fields[c].IsNull(index))
                    {
                        array.Fields[c].Accept(this);
                        addAction.DeletionVector = (DeletionVector?)result;
                    }
                }
                else if (structType.Fields[c].Name.Equals("baseRowId", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);

                    if (result != null)
                    {
                        addAction.BaseRowId = System.Convert.ToInt64(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("defaultRowCommitVersion", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);

                    if (result != null)
                    {
                        addAction.DefaultRowCommitVersion = System.Convert.ToInt64(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("clusteringProvider", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    addAction.ClusteringProvider = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("stats", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    addAction.Statistics = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("stats_parsed", StringComparison.OrdinalIgnoreCase))
                {
                    parsedRecordCount = ReadParsedRecordCount(structType.Fields[c], (StructArray)array.Fields[c], index);
                }

                fieldStack.Pop();
            }
            // Without JSON stats the parsed record count still describes the file
            if (addAction.Statistics == null && parsedRecordCount.HasValue)
            {
                addAction.Statistics = "{\"numRecords\":" + parsedRecordCount.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) + "}";
            }
            result = addAction;
        }

        private long? ReadParsedRecordCount(Field field, StructArray array, int index)
        {
            if (array.IsNull(index))
            {
                return null;
            }
            var structType = (StructType)field.DataType;
            for (int c = 0; c < structType.Fields.Count; c++)
            {
                if (structType.Fields[c].Name.Equals("numRecords", StringComparison.OrdinalIgnoreCase))
                {
                    fieldStack.Push(structType.Fields[c]);
                    array.Fields[c].Accept(this);
                    fieldStack.Pop();
                    return result != null ? System.Convert.ToInt64(result) : null;
                }
            }
            return null;
        }

        private void VisitRemove(Field field, StructArray array)
        {
            var index = indexStack.Peek();
            DeltaRemoveFileAction removeAction = new DeltaRemoveFileAction();
            var structType = (field.DataType as StructType)!;

            for (int c = 0; c < structType.Fields.Count; c++)
            {
                var name = structType.Fields[c].Name;
                fieldStack.Push(structType.Fields[c]);

                if (name.Equals("path", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.Path = (string?)result;
                }
                else if (name.Equals("deletionTimestamp", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.DeletionTimestamp = result != null ? System.Convert.ToInt64(result) : null;
                }
                else if (name.Equals("dataChange", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        removeAction.DataChange = System.Convert.ToBoolean(result);
                    }
                }
                else if (name.Equals("extendedFileMetadata", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        removeAction.ExtendedFileMetadata = System.Convert.ToBoolean(result);
                    }
                }
                else if (name.Equals("partitionValues", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.PartitionValues = (Dictionary<string, string>?)result;
                }
                else if (name.Equals("size", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.Size = result != null ? System.Convert.ToInt64(result) : null;
                }
                else if (name.Equals("stats", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.Stats = (string?)result;
                }
                else if (name.Equals("tags", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.Tags = (Dictionary<string, string>?)result;
                }
                else if (name.Equals("deletionVector", StringComparison.OrdinalIgnoreCase))
                {
                    if (!array.Fields[c].IsNull(index))
                    {
                        array.Fields[c].Accept(this);
                        removeAction.DeletionVector = (DeletionVector?)result;
                    }
                }
                else if (name.Equals("baseRowId", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.BaseRowId = result != null ? System.Convert.ToInt64(result) : null;
                }
                else if (name.Equals("defaultRowCommitVersion", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    removeAction.DefaultRowCommitVersion = result != null ? System.Convert.ToInt64(result) : null;
                }

                fieldStack.Pop();
            }
            result = removeAction;
        }

        private void VisitTxn(Field field, StructArray array)
        {
            DeltaTransactionAction transactionAction = new DeltaTransactionAction();
            var structType = (field.DataType as StructType)!;

            for (int c = 0; c < structType.Fields.Count; c++)
            {
                var name = structType.Fields[c].Name;
                fieldStack.Push(structType.Fields[c]);

                if (name.Equals("appId", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    transactionAction.AppId = (string?)result;
                }
                else if (name.Equals("version", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        transactionAction.Version = System.Convert.ToInt64(result);
                    }
                }
                else if (name.Equals("lastUpdated", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    transactionAction.LastUpdated = result != null ? System.Convert.ToInt64(result) : null;
                }

                fieldStack.Pop();
            }
            result = transactionAction;
        }

        private void VisitDomainMetadata(Field field, StructArray array)
        {
            DeltaDomainMetadataAction domainMetadataAction = new DeltaDomainMetadataAction();
            var structType = (field.DataType as StructType)!;

            for (int c = 0; c < structType.Fields.Count; c++)
            {
                var name = structType.Fields[c].Name;
                fieldStack.Push(structType.Fields[c]);

                if (name.Equals("domain", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    domainMetadataAction.Domain = (string?)result;
                }
                else if (name.Equals("configuration", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    domainMetadataAction.Configuration = (string?)result;
                }
                else if (name.Equals("removed", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        domainMetadataAction.Removed = System.Convert.ToBoolean(result);
                    }
                }

                fieldStack.Pop();
            }
            result = domainMetadataAction;
        }

        private void VisitDeletionVector(Field field, StructArray array)
        {
            DeletionVector deletionVector = new DeletionVector();
            
            
            var structType = (field.DataType as StructType)!;
            for (int c = 0; c < structType.Fields.Count; c++)
            {
                fieldStack.Push(structType.Fields[c]);

                if (structType.Fields[c].Name.Equals("storageType", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deletionVector.StorageType = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("pathOrInlineDv", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deletionVector.PathOrInlineDv = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("offset", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        deletionVector.Offset = System.Convert.ToInt64(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("sizeInBytes", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        deletionVector.SizeInBytes = System.Convert.ToInt32(result);
                    }
                }
                else if (structType.Fields[c].Name.Equals("cardinality", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    if (result != null)
                    {
                        deletionVector.Cardinality = System.Convert.ToInt64(result);
                    }
                }

                fieldStack.Pop();
            }
            result = deletionVector;

        }

        private void VisitProtocol(Field field, StructArray array)
        {
            DeltaProtocolAction deltaProtocolAction = new DeltaProtocolAction();
            var structType = (field.DataType as StructType)!;

            for (int c = 0; c < structType.Fields.Count; c++)
            {
                fieldStack.Push(structType.Fields[c]);
                if (structType.Fields[c].Name.Equals("MinReaderVersion", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaProtocolAction.MinReaderVersion = System.Convert.ToInt32(val);
                    }
                }
                else if (structType.Fields[c].Name.Equals("MinWriterVersion", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaProtocolAction.MinWriterVersion = System.Convert.ToInt32(val);
                    }
                }
                else if (structType.Fields[c].Name.Equals("ReaderFeatures", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaProtocolAction.ReaderFeatures = (List<string>)val;
                    }
                }
                else if (structType.Fields[c].Name.Equals("writerFeatures", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaProtocolAction.WriterFeatures = (List<string>)val;
                    }
                }
                fieldStack.Pop();
            }

            result = deltaProtocolAction;
        }

        private void VisitMetadata(Field field, StructArray array)
        {
            DeltaMetadataAction deltaMetadataAction = new DeltaMetadataAction();
            var structType = (field.DataType as StructType)!;
            
            for (int c = 0; c < structType.Fields.Count; c++)
            {
                fieldStack.Push(structType.Fields[c]);
                if (structType.Fields[c].Name.Equals("id", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataAction.Id = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("name", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataAction.Name = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("description", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataAction.Description = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("format", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataAction.Format = (DeltaMetadataFormat?)result;
                }
                else if (structType.Fields[c].Name.Equals("schemaString", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataAction.SchemaString = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("partitionColumns", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaMetadataAction.PartitionColumns = (List<string>?)val;
                    }
                }
                else if (structType.Fields[c].Name.Equals("configuration", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaMetadataAction.Configuration = (Dictionary<string, string>?)val;
                    }
                }
                else if (structType.Fields[c].Name.Equals("createdTime", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);

                    if (result != null)
                    {
                        deltaMetadataAction.CreatedTime = System.Convert.ToInt64(result);
                    }
                }
                fieldStack.Pop();
            }

            result = deltaMetadataAction;
        }

        private void VisitFormat(Field field, StructArray array)
        {
            DeltaMetadataFormat deltaMetadataFormat = new DeltaMetadataFormat();
            var structType = (field.DataType as StructType)!;
            for (int c = 0; c < structType.Fields.Count; c++)
            {
                fieldStack.Push(structType.Fields[c]);
                if (structType.Fields[c].Name.Equals("provider", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataFormat.Provider = (string?)result;
                }
                else if (structType.Fields[c].Name.Equals("options", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    deltaMetadataFormat.Options = (Dictionary<string, string>?)result;
                }
                fieldStack.Pop();
            }
            result = deltaMetadataFormat;
        }

        public DeltaAction? GetAction(IArrowArray array)
        {
            DeltaAction action = new DeltaAction();
            array.Accept(this);

            if (result is DeltaAddAction addAction)
            {
                action.Add = addAction;
            }
            else if (result is DeltaMetadataAction metadata)
            {
                action.MetaData = metadata;
            }
            else if (result is DeltaProtocolAction protocol)
            {
                action.Protocol = protocol;
            }
            else if (result is DeltaRemoveFileAction remove)
            {
                action.Remove = remove;
            }
            else if (result is DeltaTransactionAction transaction)
            {
                action.Txn = transaction;
            }
            else if (result is DeltaDomainMetadataAction domainMetadata)
            {
                action.DomainMetadata = domainMetadata;
            }
            else
            {
                return default;
            }
            return action;
        }


        // Arrow sends every array type without its own Visit here, accepting again would recurse forever
        public void Visit(IArrowArray array)
        {
            var field = fieldStack.Peek();
            throw new CheckpointUnusableException($"Checkpoint field '{field.Name}' has unsupported type {array.Data.DataType.Name}.");
        }

        public void Visit(Int32Array array)
        {
            var index = indexStack.Peek();

            var val = array.GetValue(index);

            if (val.HasValue)
            {
                result = val.Value;
                return;
            }
            else
            {
                result = null;
            }
        }

        public void Visit(ListArray array)
        {
            var index = indexStack.Peek();
            // A null list, such as the feature lists of an older protocol, stays null
            if (array.IsNull(index))
            {
                result = null;
                return;
            }
            var field = fieldStack.Peek();
            var offset = array.ValueOffsets[index];
            var length = array.GetValueLength(index);

            var listType = (ListType)field.DataType;
            var innerField = listType.Fields[0];
            fieldStack.Push(innerField);

            if (innerField.DataType is StringType)
            {
                List<string> values = new List<string>();
                for (var i = 0; i < length; i++)
                {
                    indexStack.Push(offset + i);
                    array.Values.Accept(this);

                    if (result != null)
                    {
                        values.Add((string)result);
                    }
                    
                    indexStack.Pop();
                }
                result = values;
            }
            else
            {
                throw new InvalidOperationException($"Unsupported list element type: {innerField.DataType.GetType().Name}. Only StringType is currently supported.");
            }

            fieldStack.Pop();
        }

        public void Visit(StringArray array)
        {
            var index = indexStack.Peek();

            var val = array.GetString(index);

            result = val;
        }

        public void Visit(MapArray array)
        {
            var field = fieldStack.Peek();
            var index = indexStack.Peek();

            var mapType = (MapType)field.DataType;
            
            if (mapType.KeyField.DataType is StringType &&
                mapType.ValueField.DataType is StringType)
            {
                Dictionary<string, string> vals = new Dictionary<string, string>();

                var offset = array.ValueOffsets[index];
                var length = array.GetValueLength(index);

                for (var i = 0; i < length; i++)
                {
                    // Key
                    fieldStack.Push(mapType.KeyField);
                    indexStack.Push(offset + i);
                    array.Keys.Accept(this);
                    var key = (string?)result;
                    indexStack.Pop();
                    fieldStack.Pop();
                    // Value
                    fieldStack.Push(mapType.ValueField);
                    indexStack.Push(offset + i);
                    array.Values.Accept(this);
                    var value = (string?)result;
                    indexStack.Pop();
                    fieldStack.Pop();

                    // Check that both key and value is not null
                    // This check is required since either one can be null when fetched from the result field
                    // after the Accept call above.
                    if (key != null && value != null)
                    {
                        vals[key] = value;
                    }
                }

                result = vals;
            }
            else
            {
                throw new NotImplementedException(
                    $"Unsupported map type: Key={mapType.KeyField.DataType.GetType().Name}, " +
                    $"Value={mapType.ValueField.DataType.GetType().Name}. Only string-to-string maps are currently supported.");
            }
        }

        public void Visit(Int64Array array)
        {
            var index = indexStack.Peek();

            var val = array.GetValue(index);

            if (val.HasValue)
            {
                result = val.Value;
                return;
            }
            else
            {
                result = null;
            }
        }

        public void Visit(BooleanArray array)
        {
            var index = indexStack.Peek();
            var val = array.GetValue(index);
            if (val.HasValue)
            {
                result = val.Value;
                return;
            }
            else
            {
                result = null;
            }
        }
    }
}
