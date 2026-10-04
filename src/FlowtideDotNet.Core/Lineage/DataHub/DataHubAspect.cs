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

using System.Text.Json.Nodes;

namespace FlowtideDotNet.Core.Lineage.DataHub
{
    /// <summary>
    /// Named DataHub aspect value in Pegasus JSON.
    /// </summary>
    public sealed class DataHubAspect
    {
        /// <summary>
        /// Creates an aspect.
        /// </summary>
        /// <param name="name">Aspect name, such as ownership or globalTags.</param>
        /// <param name="value">Aspect value as DataHub returns it from GMS.</param>
        public DataHubAspect(string name, JsonNode value)
        {
            ArgumentException.ThrowIfNullOrEmpty(name);
            ArgumentNullException.ThrowIfNull(value);
            Name = name;
            Value = value;
        }

        /// <summary>
        /// Aspect name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Aspect value in Pegasus JSON.
        /// </summary>
        public JsonNode Value { get; }
    }
}
