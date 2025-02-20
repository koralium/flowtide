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
    internal class ParquetStringWriter : IParquetWriter
    {
        private StringArray.Builder? _arrayBuilder;
        private byte[]? _minValue;
        private byte[]? _maxValue;
        private int _nullCount;

        public IArrowArray GetArray()
        {
            Debug.Assert(_arrayBuilder != null);
            return _arrayBuilder.Build();
        }

        public IDataValue? GetMaxValue()
        {
            if (_maxValue == null)
            {
                return null;
            }
            return new StringValue(_maxValue);
        }

        public IDataValue? GetMinValue()
        {
            if (_minValue == null)
            {
                return null;
            }
            return new StringValue(_minValue);
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new StringStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public void NewBatch()
        {
            _arrayBuilder = new StringArray.Builder();
            _minValue = null;
            _maxValue = null;
            _nullCount = 0;
        }

        public void WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_arrayBuilder != null);
            if (value.IsNull)
            {
                _nullCount++;
                _arrayBuilder.AppendNull();
            }
            else
            {
                if (_minValue == null)
                {
                    _minValue = value.AsString.Span.ToArray();
                }
                else if (_minValue.AsSpan().SequenceCompareTo(value.AsString.Span) > 0)
                {
                    _minValue = value.AsString.Span.ToArray();
                }
                
                if (_maxValue == null)
                {
                    _maxValue = value.AsString.Span.ToArray();
                }
                else if (_maxValue.AsSpan().SequenceCompareTo(value.AsString.Span) < 0)
                {
                    _maxValue = value.AsString.Span.ToArray();
                }

                _arrayBuilder.Append(value.AsString.Span);
            }
        }
    }
}
