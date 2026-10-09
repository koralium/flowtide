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

using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal static class LineageStreamNames
    {
        public static string GetLogicalStreamName(string builderStreamName, string? substreamName)
        {
            if (substreamName != null && TryParseSubstreamBuilderName(builderStreamName, substreamName, out var streamName))
            {
                return streamName;
            }
            return builderStreamName;
        }

        // Inverse of the "{len}_{name}_{substream}" substream builder name.
        internal static bool TryParseSubstreamBuilderName(string builderName, string substreamName, [NotNullWhen(true)] out string? streamName)
        {
            streamName = null;
            var separator = builderName.IndexOf('_');
            if (separator <= 0 ||
                !int.TryParse(builderName.AsSpan(0, separator), NumberStyles.None, CultureInfo.InvariantCulture, out var length) ||
                length < 1 ||
                length > builderName.Length - separator - 1)
            {
                return false;
            }

            var candidate = builderName.Substring(separator + 1, length);
            // Round trip rejects leading zeros and wrong suffixes.
            var expected = $"{candidate.Length.ToString(CultureInfo.InvariantCulture)}_{candidate}_{substreamName}";
            if (!string.Equals(expected, builderName, StringComparison.Ordinal))
            {
                return false;
            }
            streamName = candidate;
            return true;
        }
    }
}
