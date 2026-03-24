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
    internal class ParquetBinaryWriter : IParquetWriter
    {
        private BinaryArray.Builder? _arrayBuilder;
        private byte[]? _minValue;
        private byte[]? _maxValue;
        private int _nullCount;

        public long CopyArray(IArrowArray array, int globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            if (array is BinaryArray arr)
            {
                int addedCount = 0;
                long writtenBytes = 0;
                for (int i = index; i < (index + count); i++)
                {
                    if (deleteVector.Contains(globalOffset + i))
                    {
                        continue;
                    }
                    addedCount++;
                    if (arr.IsNull(i))
                    {
                        WriteNull();
                    }
                    else
                    {
                        var val = arr.GetBytes(i);
                        writtenBytes  += val.Length;
                        WriteValue(val);
                    }
                }
                return (addedCount * 4) + writtenBytes;
            }
            throw new NotImplementedException();
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_arrayBuilder != null);
            return _arrayBuilder.Build();
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new StringStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public void NewBatch()
        {
            _arrayBuilder = new BinaryArray.Builder();
            _minValue = null;
            _maxValue = null;
            _nullCount = 0;
        }

        public void WriteNull()
        {
            Debug.Assert(_arrayBuilder != null);
            _nullCount++;
            _arrayBuilder.AppendNull();
        }

        private long WriteValue(in ReadOnlySpan<byte> value)
        {
            Debug.Assert(_arrayBuilder != null);
            if (_minValue == null)
            {
                _minValue = value.ToArray();
            }
            else if (_minValue.AsSpan().SequenceCompareTo(value) > 0)
            {
                _minValue = value.ToArray();
            }

            if (_maxValue == null)
            {
                _maxValue = value.ToArray();
            }
            else if (_maxValue.AsSpan().SequenceCompareTo(value) < 0)
            {
                _maxValue = value.ToArray();
            }

            _arrayBuilder.Append(value);
            return value.Length + 4;
        }

        public long WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_arrayBuilder != null);
            if (value.IsNull)
            {
                _nullCount++;
                _arrayBuilder.AppendNull();
                return 4;
            }
            else
            {
                return WriteValue(value.AsBinary);
            }
        }
    }
}
