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

using FlowtideDotNet.Core.ColumnStore;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers
{
    internal class DateStatisticsComparer : IStatisticsComparer, IPruningBounds
    {
        private DateTime? _minValue;
        private DateTime? _maxValue;
        private readonly int? _nullCount;

        public DateStatisticsComparer(DateTime? minValue, DateTime? maxValue, int? nullCount)
        {
            // Bounds and probes compare on the stored date part
            _minValue = minValue.HasValue ? StoredValue.Date(minValue.Value) : null;
            _maxValue = maxValue.HasValue ? StoredValue.Date(maxValue.Value) : null;
            _nullCount = nullCount;
        }

        public bool IsInBetween<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return PruningKernels.NullMayMatch(_nullCount);
            }
            return PruningKernels.Int64(_minValue.HasValue, _minValue?.Ticks ?? 0, _maxValue.HasValue, _maxValue?.Ticks ?? 0, PruningKernels.DateProbe(value));
        }

        public void WriteBounds(PruningType type, Span<byte> cell)
        {
            if (type == PruningType.Date)
            {
                PruningCell.WriteInt64(cell, type, _minValue?.Ticks, _maxValue?.Ticks, _nullCount);
            }
            else
            {
                PruningCell.WriteUnknown(cell);
            }
        }

        public void WriteMinValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_minValue != null)
            {
                writer.WriteString(propertyName, _minValue.Value.ToString("yyyy-MM-dd"));
            }
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_maxValue != null)
            {
                writer.WriteString(propertyName, _maxValue.Value.ToString("yyyy-MM-dd"));
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
