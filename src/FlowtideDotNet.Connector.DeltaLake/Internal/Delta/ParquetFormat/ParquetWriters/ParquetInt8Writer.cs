﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    internal class ParquetInt8Writer : IParquetWriter
    {
        private Int8Array.Builder? _builder;
        private long? _minValue;
        private long? _maxValue;
        private int _nullCount;

        public void CopyArray(IArrowArray array, int globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            if (array is Int8Array arr)
            {
                for (int i = index; i < (index + count); i++)
                {
                    if (deleteVector.Contains(globalOffset + i))
                    {
                        continue;
                    }

                    var val = arr.GetValue(i);
                    if (!val.HasValue)
                    {
                        WriteNull();
                    }
                    else
                    {
                        WriteValue(val.Value);
                    }
                }
                return;
            }
            throw new NotImplementedException();
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_builder != null);
            return _builder.Build();
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new Int64StatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public void NewBatch()
        {
            _builder = new Int8Array.Builder();
            _minValue = null;
            _maxValue = null;
            _nullCount = 0;
        }

        public void WriteNull()
        {
            Debug.Assert(_builder != null);
            _nullCount++;
            _builder.AppendNull();
        }

        private void WriteValue(long val)
        {
            Debug.Assert(_builder != null);
            if (_minValue == null || val < _minValue)
            {
                _minValue = val;
            }
            if (_maxValue == null || val > _maxValue)
            {
                _maxValue = val;
            }

            _builder.Append((sbyte)val);
        }

        public void WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_builder != null);
            if (value.IsNull)
            {
                _nullCount++;
                _builder.AppendNull();
                return;
            }

            var val = value.AsLong;

            if (_minValue == null || val < _minValue)
            {
                _minValue = val;
            }
            if (_maxValue == null || val > _maxValue)
            {
                _maxValue = val;
            }

            _builder.Append((sbyte)val);
        }
    }
}
