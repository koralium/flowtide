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
using System.Buffers.Binary;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    internal class ParquetDecimalWriter : IParquetWriter
    {
        private readonly Decimal128Type _type;
        private Decimal128Array.Builder? _builder;
        private decimal? _minValue;
        private decimal? _maxValue;
        private int _nullCount;
        // Copied extrema as raw values, any number of digits
        private Int128? _minCopied;
        private Int128? _maxCopied;


        public ParquetDecimalWriter(int precision, int scale)
        {
            _type = new Decimal128Type(precision, scale);
        }

        public long CopyArray(IArrowArray array, long globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            Debug.Assert(_builder != null);
            if (array is Decimal128Array arr)
            {
                int added = 0;
                for (int i = index; i < (index + count); i++)
                {
                    if (deleteVector.Contains(globalOffset + i))
                    {
                        continue;
                    }
                    added++;
                    if (arr.IsNull(i))
                    {
                        WriteNull();
                        continue;
                    }
                    var bytes = arr.GetBytes(i);
                    _builder.Append(bytes);
                    var value = BinaryPrimitives.ReadInt128LittleEndian(bytes);
                    if (_minCopied == null || value < _minCopied)
                    {
                        _minCopied = value;
                    }
                    if (_maxCopied == null || value > _maxCopied)
                    {
                        _maxCopied = value;
                    }
                }
                return added * 16;
            }
            throw new NotImplementedException();
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_builder != null);
            return _builder.Build();
        }

        // No bounds when a copied extremum has no exact System.Decimal
        public IStatisticsComparer GetStatisticsComparer()
        {
            var min = _minValue;
            var max = _maxValue;
            if (_minCopied != null)
            {
                var copiedMin = ToDecimal(_minCopied.Value, _type.Scale);
                var copiedMax = ToDecimal(_maxCopied!.Value, _type.Scale);
                if (copiedMin == null || copiedMax == null)
                {
                    return new DecimalStatisticsComparer(null, null, _nullCount);
                }
                min = min == null || copiedMin < min ? copiedMin : min;
                max = max == null || copiedMax > max ? copiedMax : max;
            }
            return new DecimalStatisticsComparer(min, max, _nullCount);
        }

        // The exact System.Decimal, or null when it has none
        private static decimal? ToDecimal(Int128 unscaled, int scale)
        {
            var negative = unscaled < 0;
            var magnitude = negative ? (UInt128)(-(unscaled + 1)) + 1 : (UInt128)unscaled;
            while (scale > 28 || magnitude >> 96 != 0)
            {
                if (scale == 0 || magnitude % 10 != 0)
                {
                    return null;
                }
                magnitude /= 10;
                scale--;
            }
            return new decimal((int)(uint)magnitude, (int)(uint)(magnitude >> 32), (int)(uint)(magnitude >> 64), negative, (byte)scale);
        }

        public void NewBatch()
        {
            _builder = new Decimal128Array.Builder(_type);
            _minValue = null;
            _maxValue = null;
            _nullCount = 0;
            _minCopied = null;
            _maxCopied = null;
        }

        public void WriteNull()
        {
            Debug.Assert(_builder != null);
            _nullCount++;
            _builder.AppendNull();
        }

        private void WriteValue(decimal decimalValue)
        {
            Debug.Assert(_builder != null);
            decimalValue = Math.Round(decimalValue, _type.Scale);
            Track(decimalValue);
            _builder.Append(decimalValue);
        }

        private void Track(decimal decimalValue)
        {
            if (_minValue == null || _minValue.Value.CompareTo(decimalValue) > 0)
            {
                _minValue = decimalValue;
            }
            if (_maxValue == null || _maxValue.Value.CompareTo(decimalValue) < 0)
            {
                _maxValue = decimalValue;
            }
        }

        public long WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_builder != null);
            if (value.IsNull)
            {
                _nullCount++;
                _builder.AppendNull();
                return 16;
            }
            var decimalValue = value.AsDecimal;
            WriteValue(decimalValue);
            return 16;
        }
    }
}
