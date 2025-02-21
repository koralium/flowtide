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
    internal class ParquetFloat32Writer : IParquetWriter
    {
        private FloatArray.Builder? _builder;
        private double? _minValue;
        private double? _maxValue;
        private int _nullCount;

        public IArrowArray GetArray()
        {
            Debug.Assert(_builder != null);
            return _builder.Build();
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new FloatStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public void NewBatch()
        {
            _builder = new FloatArray.Builder();
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

            var val = value.AsDouble;

            if (_minValue == null || val < _minValue)
            {
                _minValue = val;
            }

            if (_maxValue == null || val > _maxValue)
            {
                _maxValue = val;
            }

            _builder.Append((float)val);
        }
    }
}
