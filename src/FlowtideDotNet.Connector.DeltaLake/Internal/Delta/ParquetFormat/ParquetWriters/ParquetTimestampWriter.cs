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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    // Raw longs in one unit, milliseconds or microseconds
    internal class ParquetTimestampWriter : IParquetWriter
    {
        private static readonly long s_minMilliseconds = DateTimeOffset.MinValue.ToUnixTimeMilliseconds();
        private static readonly long s_maxMilliseconds = DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();
        private readonly TimeUnit _unit;
        private ArrowBuffer.Builder<long>? _values;
        private ArrowBuffer.BitmapBuilder? _validity;
        private long? _minValue;
        private long? _maxValue;
        private int _nullCount;

        public ParquetTimestampWriter(TimeUnit unit = TimeUnit.Millisecond)
        {
            _unit = unit;
        }

        public long CopyArray(IArrowArray array, long globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            if (array is TimestampArray arr)
            {
                var unit = ((TimestampType)arr.Data.DataType).Unit;
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
                    }
                    else
                    {
                        Append(Scale(arr.Values[i], unit));
                    }
                }
                return added * 8;
            }
            throw new NotImplementedException();
        }

        // Exact, or an error
        private long Scale(long value, TimeUnit unit)
        {
            if (unit == _unit)
            {
                return value;
            }
            return (unit, _unit) switch
            {
                (TimeUnit.Second, TimeUnit.Millisecond) => checked(value * 1_000),
                (TimeUnit.Second, TimeUnit.Microsecond) => checked(value * 1_000_000),
                (TimeUnit.Millisecond, TimeUnit.Microsecond) => checked(value * 1_000),
                (TimeUnit.Nanosecond, TimeUnit.Microsecond) when value % 1_000 == 0 => value / 1_000,
                _ => throw new InvalidOperationException($"Timestamp {value} in {unit} cannot be written exactly in {_unit}")
            };
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_values != null && _validity != null);
            return new TimestampArray(new TimestampType(_unit, "UTC"), _values.Build(), _validity.Build(), _validity.Length, _nullCount, 0);
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new TimestampStatisticsComparer(Bound(_minValue), Bound(_maxValue), _nullCount);
        }

        // Bounds are the stored values floored to milliseconds, none past DateTimeOffset
        private DateTimeOffset? Bound(long? value)
        {
            if (value == null)
            {
                return null;
            }
            var milliseconds = _unit == TimeUnit.Microsecond ? Math.DivRem(value.Value, 1_000, out var remainder) - (remainder < 0 ? 1 : 0) : value.Value;
            if (milliseconds < s_minMilliseconds || milliseconds > s_maxMilliseconds)
            {
                return null;
            }
            return DateTimeOffset.UnixEpoch.AddTicks(milliseconds * TimeSpan.TicksPerMillisecond);
        }

        public void NewBatch()
        {
            _values = new ArrowBuffer.Builder<long>();
            _validity = new ArrowBuffer.BitmapBuilder();
            _minValue = null;
            _maxValue = null;
            _nullCount = 0;
        }

        public void WriteNull()
        {
            Debug.Assert(_values != null && _validity != null);
            _nullCount++;
            _values.Append(0);
            _validity.Append(false);
        }

        private void Append(long value)
        {
            Debug.Assert(_values != null && _validity != null);
            if (_minValue == null || value < _minValue)
            {
                _minValue = value;
            }
            if (_maxValue == null || value > _maxValue)
            {
                _maxValue = value;
            }
            _values.Append(value);
            _validity.Append(true);
        }

        // Inserts write milliseconds
        public long WriteValue<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                WriteNull();
                return 8;
            }

            Debug.Assert(_unit == TimeUnit.Millisecond);
            Append(StoredValue.TimestampMilliseconds(value.AsTimestamp));
            return 8;
        }
    }
}
