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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using System.Text.Json;
using System.Text.Unicode;

namespace FlowtideDotNet.Connector.DeltaLake.Tests.PruningOracle
{
    // Frozen copy of the comparer before the pruning kernels, the oracle for the kernels
    internal class OracleBinaryComparer
    {
        private byte[]? _minValue;
        private byte[]? _maxValue;
        private int? _nullCount;

        public OracleBinaryComparer(byte[]? minValue, byte[]? maxValue, int? nullCount)
        {
            _minValue = minValue;
            _maxValue = maxValue;
            _nullCount = nullCount;
        }

        public bool IsInBetween<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                if ((!_nullCount.HasValue) || _nullCount.Value > 0)
                {
                    return true;
                }
                return false;
            }
            var str = value.AsBinary;

            if (_minValue != null && _minValue.AsSpan().SequenceCompareTo(str) > 0)
            {
                return false;
            }
            if (_maxValue != null && _maxValue.AsSpan().SequenceCompareTo(str) < 0)
            {
                return false;
            }
            return true;
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            // Json text cannot hold invalid utf8, leave unbounded
            if (_maxValue != null && Utf8.IsValid(_maxValue))
            {
                writer.WriteString(propertyName, _maxValue);
            }
        }

        public void WriteMinValue(Utf8JsonWriter writer, string propertyName)
        {
            // Json text cannot hold invalid utf8, leave unbounded
            if (_minValue != null && Utf8.IsValid(_minValue))
            {
                writer.WriteString(propertyName, _minValue);
            }
        }

        public void WriteNullValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_nullCount.HasValue)
            {
                writer.WriteNumber(propertyName, _nullCount.Value);
            }
        }
    }
}
