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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The two table properties that publish the history retention, and the one rule for whether a table carries them.
    /// </summary>
    internal static class RetentionProperties
    {
        public const string LogKey = "delta.logRetentionDuration";

        public const string DeletedFileKey = CheckpointExpiry.RetentionProperty;

        /// <summary>
        /// The published value of a validated retention: "interval {wholeHours} hours".
        /// </summary>
        public static string Format(TimeSpan validated)
        {
            return string.Create(CultureInfo.InvariantCulture, $"interval {validated.Ticks / TimeSpan.TicksPerHour} hours");
        }

        /// <summary>
        /// Whether both keys are present with exactly the formatted value; keys and values compare ordinally.
        /// </summary>
        public static bool Matches(IReadOnlyDictionary<string, string>? configuration, string formatted)
        {
            return configuration != null
                && configuration.TryGetValue(LogKey, out var log) && string.Equals(log, formatted, StringComparison.Ordinal)
                && configuration.TryGetValue(DeletedFileKey, out var deletedFile) && string.Equals(deletedFile, formatted, StringComparison.Ordinal);
        }
    }
}
