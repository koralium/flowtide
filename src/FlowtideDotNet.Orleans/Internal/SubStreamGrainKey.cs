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

namespace FlowtideDotNet.Orleans.Internal
{
    /// <summary>
    /// Builds the grain key of a substream grain from the stream and substream name.
    /// Every construction of the key must go through here so all callers reach the same
    /// grain.
    /// </summary>
    internal static class SubStreamGrainKey
    {
        /// <summary>
        /// The stream name length prefix makes the key injective: a plain
        /// "{stream}_{substream}" would map stream "orders" with substream "eu_west" and
        /// stream "orders_eu" with substream "west" to the same grain, silently mixing the
        /// state of two different streams.
        /// </summary>
        public static string Create(string streamName, string substreamName)
        {
            return $"{streamName.Length}_{streamName}_{substreamName}";
        }

        /// <summary>
        /// Parses a key created by <see cref="Create"/>. Returns false for keys in another
        /// format, for example keys persisted in reminder tables by versions that used a
        /// key without the length prefix, such activations must clean themselves up instead
        /// of running a substream that stop and delete can no longer reach.
        /// </summary>
        public static bool TryParse(string key, out string streamName, out string substreamName)
        {
            streamName = string.Empty;
            substreamName = string.Empty;
            var lengthEnd = key.IndexOf('_');
            if (lengthEnd <= 0 || !int.TryParse(key.AsSpan(0, lengthEnd), out var streamNameLength) || streamNameLength < 1)
            {
                return false;
            }
            var streamStart = lengthEnd + 1;
            // The separator after the stream name and at least one substream name character
            var substreamStart = streamStart + streamNameLength + 1;
            if (substreamStart >= key.Length || key[substreamStart - 1] != '_')
            {
                return false;
            }
            streamName = key.Substring(streamStart, streamNameLength);
            substreamName = key.Substring(substreamStart);
            return true;
        }
    }
}
