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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    // A struct field's intake name, its output name and id, and its writer
    internal readonly record struct StructFieldWriter(string Key, string OutputName, int? FieldId, IParquetWriter Value);

    internal class ParquetStructWriter : IParquetWriter
    {
        private readonly List<StructFieldWriter> propertyWriters;
        private List<int> order;
        // Values match fields by name, in any order
        private readonly Dictionary<string, int> _indexByKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly bool[] _written;
        ArrowBuffer.BitmapBuilder? _nullBitmap;
        private int _nullCount;


        public ParquetStructWriter(IEnumerable<StructFieldWriter> propertyWriters)
        {
            var keys = propertyWriters.Select(x => x.Key).ToList();
            this.propertyWriters = propertyWriters.OrderBy(x => x.Key).ToList();
            for (int i = 0; i < this.propertyWriters.Count; i++)
            {
                if (!_indexByKey.TryAdd(this.propertyWriters[i].Key, i))
                {
                    throw new InvalidOperationException($"Struct field {this.propertyWriters[i].Key} is in the table more than once.");
                }
            }
            _written = new bool[this.propertyWriters.Count];

            order = new List<int>();
            foreach (var key in keys)
            {
                for (int i = 0; i < this.propertyWriters.Count; i++)
                {
                    if (this.propertyWriters[i].Key == key)
                    {
                        order.Add(i);
                        break;
                    }
                }
            }
        }

        public long CopyArray(IArrowArray array, long globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            Debug.Assert(_nullBitmap != null);
            if (array is StructArray arr)
            {
                int addedCount = 0;
                long addedBytes = 0;
                for (int j = 0; j < propertyWriters.Count; j++)
                {
                    var field = arr.Fields[j];
                    addedBytes += propertyWriters[order[j]].Value.CopyArray(field, globalOffset, deleteVector, index, count);
                }
                // The children are copied, so only the validity bit
                for (int i = index; i < (index + count); i++)
                {
                    if (deleteVector.Contains(globalOffset + i))
                    {
                        continue;
                    }
                    
                    addedCount++;
                    var valid = arr.IsValid(i);
                    _nullBitmap.Append(valid);
                    if (!valid)
                    {
                        _nullCount++;
                    }
                }
                return addedCount + addedBytes;
            }
            throw new NotImplementedException();
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_nullBitmap != null);
            List<IArrowArray> arrays = new List<IArrowArray>();
            List<Apache.Arrow.Field> fields = new List<Field>();

            for (int i = 0; i < order.Count; i++)
            {
                var writer = propertyWriters[order[i]];
                var arr = writer.Value.GetArray();
                arrays.Add(arr);
                fields.Add(new Apache.Arrow.Field(writer.OutputName, arr.Data.DataType, true, DeltaTypeArrowTypeVisitor.FieldIdMetadata(writer.FieldId)));
            }

            var structType = new Apache.Arrow.Types.StructType(fields);

            return new StructArray(structType, arrays[0].Length, arrays, _nullBitmap.Build(), _nullCount);
        }

        public IStatisticsComparer? GetStatisticsComparer()
        {
            List<KeyValuePair<string, IStatisticsComparer>> statisticsComparers = new List<KeyValuePair<string, IStatisticsComparer>>();
            foreach (var writer in propertyWriters)
            {
                var comparer = writer.Value.GetStatisticsComparer();
                if (comparer != null)
                {
                    statisticsComparers.Add(new KeyValuePair<string, IStatisticsComparer>(writer.OutputName, comparer));
                }
            }
            return new StructStatisticsComparer(statisticsComparers, _nullCount);
        }

        public void NewBatch()
        {
            _nullBitmap = new ArrowBuffer.BitmapBuilder();
            _nullCount = 0;
            foreach (var writer in propertyWriters)
            {
                writer.Value.NewBatch();
            }
        }

        public void WriteNull()
        {
            Debug.Assert(_nullBitmap != null);
            _nullCount++;
            _nullBitmap.Append(false);
            foreach (var writer in propertyWriters)
            {
                writer.Value.WriteNull();
            }
        }

        public long WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_nullBitmap != null);
            if (value.IsNull)
            {
                WriteNull();
                return 1;
            }

            if (value.Type == ArrowTypeId.Map)
            {
                var mapValue = value.AsMap;

                _nullBitmap.Append(true);
                System.Array.Clear(_written);
                var length = mapValue.GetLength();
                long writtenBytes = 0;
                for (int i = 0; i < length; i++)
                {
                    var index = IndexOf(mapValue.GetKeyAt(i).AsString.ToString());
                    writtenBytes += propertyWriters[index].Value.WriteValue(mapValue.GetValueAt(i));
                }
                NullTheRest();
                return writtenBytes;
            }
            else if (value.Type == ArrowTypeId.Struct)
            {
                var structValue = value.AsStruct;

                _nullBitmap.Append(true);
                System.Array.Clear(_written);
                long writtenBytes = 0;
                var length = structValue.Header.Count;
                for (int i = 0; i < length; i++)
                {
                    var index = IndexOf(structValue.Header.GetColumnName(i));
                    writtenBytes += propertyWriters[index].Value.WriteValue(structValue.GetAt(i));
                }
                NullTheRest();
                return writtenBytes;
            }
            else
            {
                throw new NotImplementedException($"StructWriter does not support type {value.Type}");
            }
        }

        // Each field the value names, once
        private int IndexOf(string name)
        {
            if (!_indexByKey.TryGetValue(name, out var index))
            {
                throw new InvalidOperationException($"Struct field {name} does not exist in the table.");
            }
            if (_written[index])
            {
                throw new InvalidOperationException($"Struct field {name} is written more than once.");
            }
            _written[index] = true;
            return index;
        }

        // Fields the value did not name are null
        private void NullTheRest()
        {
            for (int i = 0; i < _written.Length; i++)
            {
                if (!_written[i])
                {
                    propertyWriters[i].Value.WriteNull();
                }
            }
        }
    }
}
