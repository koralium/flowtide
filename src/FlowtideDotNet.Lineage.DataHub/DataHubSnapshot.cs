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

namespace FlowtideDotNet.Lineage.DataHub
{
    /// <summary>
    /// Every entity served to DataHub at one point in time.
    /// </summary>
    public sealed class DataHubSnapshot
    {
        private readonly Dictionary<string, byte[]> _entities;

        internal DataHubSnapshot(IReadOnlyList<string> urns, Dictionary<string, byte[]> entities)
        {
            Urns = urns;
            _entities = entities;
        }

        /// <summary>
        /// Entity urns in ordinal order.
        /// </summary>
        public IReadOnlyList<string> Urns { get; }

        /// <summary>
        /// Gets an entity as a GMS entitiesV2 response.
        /// </summary>
        /// <param name="urn">Entity urn, case sensitive.</param>
        /// <param name="utf8Json">UTF-8 encoded entity JSON.</param>
        /// <returns>False when the urn is not served.</returns>
        public bool TryGetEntity(string urn, out ReadOnlyMemory<byte> utf8Json)
        {
            ArgumentNullException.ThrowIfNull(urn);
            if (_entities.TryGetValue(urn, out var json))
            {
                utf8Json = json;
                return true;
            }
            utf8Json = default;
            return false;
        }
    }
}
