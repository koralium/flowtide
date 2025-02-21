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
    internal class ParquetDecimalWriter : IParquetWriter
    {
        private readonly Decimal128Type _type;
        private Decimal128Array.Builder? _builder;
        private decimal? _minValue;
        private decimal? _maxValue;
        private int _nullCount;


        public ParquetDecimalWriter(int precision, int scale)
        {
            _type = new Decimal128Type(precision, scale);
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_builder != null);
            return _builder.Build();
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new DecimalStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public void NewBatch()
        {
            new Decimal128Array.Builder(_type);
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

        public void WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_builder != null);
            if (value.IsNull)
            {
                _nullCount++;
                _builder.AppendNull();
                return;
            }
            var decimalValue = value.AsDecimal;

            if (_minValue == null || _minValue.Value.CompareTo(decimalValue) > 0)
            {
                _minValue = decimalValue;
            }
            if (_maxValue == null || _maxValue.Value.CompareTo(decimalValue) < 0)
            {
                _maxValue = decimalValue;
            }
            _builder.Append(decimalValue);
        }
    }
}
