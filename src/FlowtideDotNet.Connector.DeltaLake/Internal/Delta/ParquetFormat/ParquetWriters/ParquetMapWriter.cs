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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    internal class ParquetMapWriter : IParquetWriter
    {
        private readonly IParquetWriter _keyWriter;
        private readonly IParquetWriter _valueWriter;
        private ArrowBuffer.BitmapBuilder? _nullBitmap;
        private ArrowBuffer.Builder<int>? _offsetBuilder;
        private int _nullCount;

        public ParquetMapWriter(IParquetWriter keyWriter, IParquetWriter valueWriter)
        {
            _keyWriter = keyWriter;
            _valueWriter = valueWriter;
        }

        public void CopyArray(IArrowArray array, int globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            Debug.Assert(_nullBitmap != null);
            Debug.Assert(_offsetBuilder != null);

            if (array is MapArray arr)
            {
                for (int i = index; i < (index + count); i++)
                {
                    if (deleteVector.Contains(globalOffset + i))
                    {
                        continue;
                    }

                    if (arr.IsNull(i))
                    {
                        WriteNull();
                    }
                    else
                    {
                        var offset = arr.ValueOffsets[i];
                        var length = arr.GetValueLength(i);

                        _keyWriter.CopyArray(arr.Keys, 0, EmptyDeleteVector.Instance, offset, length);
                        _valueWriter.CopyArray(arr.Values, 0, EmptyDeleteVector.Instance, offset, length);
                        _nullBitmap.Append(true);
                        _offsetBuilder.Append(_offsetBuilder.Span[_offsetBuilder.Length - 1] + length);
                    }
                }
                return;
            }
            throw new NotImplementedException();
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_nullBitmap != null);
            Debug.Assert(_offsetBuilder != null);
            var keyArray = _keyWriter.GetArray();
            var valueArray = _valueWriter.GetArray();
            var mapType = new MapType(keyArray.Data.DataType, valueArray.Data.DataType, keySorted: true);
            var structType = new StructType(new List<Field>()
            {
                new Field("key", keyArray.Data.DataType, true),
                new Field("value", keyArray.Data.DataType, true)
            });
            var structArray = new StructArray(structType, keyArray.Length, [keyArray, valueArray], ArrowBuffer.Empty, 0, 0);
            return new MapArray(mapType, _offsetBuilder.Length - 1, _offsetBuilder.Build(), structArray, _nullBitmap.Build(), _nullCount);
        }

        public IStatisticsComparer? GetStatisticsComparer()
        {
            return default;
        }

        public void NewBatch()
        {
            _keyWriter.NewBatch();
            _valueWriter.NewBatch();
            _nullBitmap = new ArrowBuffer.BitmapBuilder();
            _offsetBuilder = new ArrowBuffer.Builder<int>();
            _offsetBuilder.Append(0);
            _nullCount = 0;
        }

        public void WriteNull()
        {
            Debug.Assert(_nullBitmap != null);
            _nullBitmap.Append(false);
            _nullCount++;
        }

        public void WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_nullBitmap != null);
            Debug.Assert(_offsetBuilder != null);
            if (value.IsNull)
            {
                WriteNull();
                return;
            }

            _nullBitmap.Append(true);
            var mapVal = value.AsMap;
            var length = mapVal.GetLength();

            for (int i = 0; i < length; i++)
            {
                var key = mapVal.GetKeyAt(i);
                _keyWriter.WriteValue(key);
                var val = mapVal.GetValueAt(i);
                _valueWriter.WriteValue(val);
            }

            _offsetBuilder.Append(_offsetBuilder.Span[_offsetBuilder.Length - 1] + length);
        }
    }
}
