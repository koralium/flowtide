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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats
{
    /// <summary>
    /// Whether a value can be inside a file's bounds, shared by the statistics comparers and the sink catalog.
    /// Bounds and probes are normalized to the stored values first.
    /// </summary>
    internal static class PruningKernels
    {
        public const double FloatEpsilon = 1e-8;
        public const decimal DecimalEpsilon = 0.000000001m;

        // A null value can only be in a file whose null count is unknown or above zero
        public static bool NullMayMatch(int? nullCount)
        {
            return !nullCount.HasValue || nullCount.Value > 0;
        }

        public static bool Int64(bool hasMin, long min, bool hasMax, long max, long probe)
        {
            return !(hasMin && min > probe) && !(hasMax && max < probe);
        }

        public static bool Double(bool hasMin, double min, bool hasMax, double max, double probe)
        {
            return !(hasMin && (min - FloatEpsilon) > probe) && !(hasMax && (max + FloatEpsilon) < probe);
        }

        public static bool Decimal(bool hasMin, decimal min, bool hasMax, decimal max, decimal probe)
        {
            return !(hasMin && (min - DecimalEpsilon) > probe) && !(hasMax && (max + DecimalEpsilon) < probe);
        }

        public static bool Bool(bool hasMin, bool min, bool hasMax, bool max, bool probe)
        {
            return !(hasMin && min.CompareTo(probe) > 0) && !(hasMax && max.CompareTo(probe) < 0);
        }

        // Ordinal byte order, the probe is the whole value
        public static bool Bytes(bool hasMin, ReadOnlySpan<byte> min, bool hasMax, ReadOnlySpan<byte> max, ReadOnlySpan<byte> probe)
        {
            return !(hasMin && min.SequenceCompareTo(probe) > 0) && !(hasMax && max.SequenceCompareTo(probe) < 0);
        }

        // Stored double for float and double columns, int64 probes are accepted like the comparer always did
        public static bool TryDoubleProbe<T>(in T value, bool isFloat32, out double probe)
            where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Double)
            {
                probe = value.AsDouble;
            }
            else if (value.Type == ArrowTypeId.Int64)
            {
                probe = value.AsLong;
            }
            else
            {
                probe = 0;
                return false;
            }
            if (isFloat32)
            {
                probe = StoredValue.Float32(probe);
            }
            return true;
        }

        // Ticks of the stored date
        public static long DateProbe<T>(in T value)
            where T : IDataValue
        {
            return StoredValue.Date(value.AsTimestamp.ToDateTimeOffset().DateTime).Ticks;
        }

        // Milliseconds of the stored timestamp
        public static long TimestampProbe<T>(in T value)
            where T : IDataValue
        {
            return StoredValue.TimestampMilliseconds(value.AsTimestamp);
        }
    }
}
