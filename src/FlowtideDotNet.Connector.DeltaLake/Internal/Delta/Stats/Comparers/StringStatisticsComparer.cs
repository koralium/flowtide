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
    internal class StringStatisticsComparer : IStatisticsComparer, IPruningBounds
    {
        // Spark's default prefix for string statistics, in code points
        internal const int StatisticsPrefixLength = 32;

        // U+10FFFF, the largest UTF-8 sequence, closes a truncated maximum
        private static readonly byte[] s_tieBreaker = { 0xF4, 0x8F, 0xBF, 0xBF };

        private byte[]? _minValue;
        private byte[]? _maxValue;
        private readonly int? _nullCount;

        public StringStatisticsComparer(byte[]? minValue, byte[]? maxValue, int? nullCount)
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
            var str = value.AsString;

            if (_minValue != null && _minValue.AsSpan().SequenceCompareTo(str.Span) > 0)
            {
                return false;
            }
            if (_maxValue != null && _maxValue.AsSpan().SequenceCompareTo(str.Span) < 0)
            {
                return false;
            }
            return true;
        }

        public void WriteBounds(PruningType type, Span<byte> cell)
        {
            if (type == PruningType.String)
            {
                PruningCell.WriteBytes(cell, _minValue, _maxValue, _nullCount);
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
                WriteMinBound(writer, propertyName, _minValue);
            }
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_maxValue != null)
            {
                WriteMaxBound(writer, propertyName, _maxValue);
            }
        }

        // A prefix is at most the value, so it stays a lower bound in UTF-8 byte order
        internal static void WriteMinBound(Utf8JsonWriter writer, string propertyName, ReadOnlySpan<byte> value)
        {
            writer.WriteString(propertyName, Prefix(value, out _));
        }

        // An upper bound in UTF-8 byte order for every value at most this valid UTF-8 one
        internal static void WriteMaxBound(Utf8JsonWriter writer, string propertyName, ReadOnlySpan<byte> value)
        {
            var prefix = Prefix(value, out var truncated);
            var rest = value.Slice(prefix.Length);
            // Untruncated, or already a prefix closed by U+10FFFF, the value is written as it is
            if (!truncated || rest.SequenceEqual(s_tieBreaker))
            {
                writer.WriteString(propertyName, value);
                return;
            }
            // The prefix and U+10FFFF bound every value sharing the prefix, unless U+10FFFF itself follows it; then no maximum is written
            if (rest.StartsWith(s_tieBreaker))
            {
                return;
            }
            Span<byte> bounded = stackalloc byte[prefix.Length + s_tieBreaker.Length];
            prefix.CopyTo(bounded);
            s_tieBreaker.CopyTo(bounded.Slice(prefix.Length));
            writer.WriteString(propertyName, bounded);
        }

        // The first StatisticsPrefixLength code points of a UTF-8 value
        internal static ReadOnlySpan<byte> Prefix(ReadOnlySpan<byte> value, out bool truncated)
        {
            var length = 0;
            for (int codePoints = 0; codePoints < StatisticsPrefixLength && length < value.Length; codePoints++)
            {
                var lead = value[length];
                length += lead < 0x80 ? 1 : lead < 0xE0 ? 2 : lead < 0xF0 ? 3 : 4;
            }
            length = Math.Min(length, value.Length);
            truncated = length < value.Length;
            return value.Slice(0, length);
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
