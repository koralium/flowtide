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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// Tombstone expiry, compared exactly in 128-bit ticks.
    /// </summary>
    internal static class CheckpointExpiry
    {
        public const string RetentionProperty = "delta.deletedFileRetentionDuration";

        public static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(7);

        /// <summary>
        /// The retention from the table's configuration: seven days when absent, a decline when present but not a valid interval.
        /// </summary>
        public static TimeSpan Retention(IReadOnlyDictionary<string, string>? configuration)
        {
            if (configuration == null || !configuration.TryGetValue(RetentionProperty, out var value))
            {
                return DefaultRetention;
            }
            return DeltaInterval.Parse(value)
                ?? throw new CheckpointDeclinedException(MaintenanceReason.InvalidRetention, DeclineDependency.Protocol, $"{RetentionProperty} is '{value}'");
        }

        public static Int128 CutoffTicks(DateTimeOffset buildNow, TimeSpan retention)
        {
            return (Int128)(buildNow.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) - retention.Ticks;
        }

        /// <summary>
        /// The cutoff in whole milliseconds, ceil(ticks / 10,000), clamped to the long range: ts &lt; cutoffMs iff ts in ticks &lt; the cutoff.
        /// </summary>
        public static long CutoffMs(Int128 cutoffTicks)
        {
            var quotient = cutoffTicks / TimeSpan.TicksPerMillisecond;
            if (quotient * TimeSpan.TicksPerMillisecond < cutoffTicks)
            {
                quotient++;
            }
            if (quotient < long.MinValue)
            {
                return long.MinValue;
            }
            return quotient > long.MaxValue ? long.MaxValue : (long)quotient;
        }

        public static bool Expired(long? deletionTimestamp, Int128 cutoffTicks)
        {
            return deletionTimestamp == null || (Int128)deletionTimestamp.Value * TimeSpan.TicksPerMillisecond < cutoffTicks;
        }
    }
}
