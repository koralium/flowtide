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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    internal class ParquetArrayWriter : IParquetWriter
    {
        private ArrowBuffer.Builder<int>? _offsetBuilder;
        private ArrowBuffer.BitmapBuilder? _nullBitmap;
        private readonly IParquetWriter _inner;
        private int _nullCount;

        public ParquetArrayWriter(IParquetWriter inner)
        {
            _inner = inner;
        }


        public void CopyArray(IArrowArray array, int globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            Debug.Assert(_nullBitmap != null);
            Debug.Assert(_offsetBuilder != null);

            if (array is ListArray arr)
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

                        _inner.CopyArray(arr.Values, 0, EmptyDeleteVector.Instance, offset, length);
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
            var innerArray = _inner.GetArray();
            var listType = new ListType(innerArray.Data.DataType);
            return new ListArray(listType, _offsetBuilder.Length - 1, _offsetBuilder.Build(), innerArray, _nullBitmap.Build(), _nullCount);
        }

        public IStatisticsComparer? GetStatisticsComparer()
        {
            return null;
        }

        public void NewBatch()
        {
            _inner.NewBatch();
            _offsetBuilder = new ArrowBuffer.Builder<int>();
            _offsetBuilder.Append(0);
            _nullBitmap = new ArrowBuffer.BitmapBuilder();
            _nullCount = 0;
        }

        public void WriteNull()
        {
            Debug.Assert(_nullBitmap != null);
            Debug.Assert(_offsetBuilder != null);
            _nullBitmap.Append(false);
            _offsetBuilder.Append(_offsetBuilder.Span[_offsetBuilder.Length - 1]);
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
            var val = value.AsList;

            for (int i = 0; i < val.Count; i++)
            {
                _inner.WriteValue(val.GetAt(i));
            }

            _offsetBuilder.Append(_offsetBuilder.Span[_offsetBuilder.Length - 1] + val.Count);
        }
    }
}
