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

                fieldStack.Pop();
            }
            result = addAction;
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
                if (structType.Fields[c].Name.Equals("MinWriterVersion", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaProtocolAction.MinWriterVersion = System.Convert.ToInt32(val);
                    }
                }
                if (structType.Fields[c].Name.Equals("ReaderFeatures", StringComparison.OrdinalIgnoreCase))
                {
                    array.Fields[c].Accept(this);
                    var val = result;

                    if (val != null)
                    {
                        deltaProtocolAction.ReaderFeatures = (List<string>)val;
                    }
                }
                if (structType.Fields[c].Name.Equals("writerFeatures", StringComparison.OrdinalIgnoreCase))
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
            else
            {
                return default;
            }
            return action;
        }


        public void Visit(IArrowArray array)
        {
            array.Accept(this);
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
                    values.Add(((string?)result)!);
                    indexStack.Pop();
                }
                result = values;
            }
            else
            {
                throw new InvalidOperationException();
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
                    if (key != null && value != null)
                    {
                        vals[key] = value;
                    }
                }

                result = vals;
            }
            else
            {
                throw new NotImplementedException();
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
