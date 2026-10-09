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
    /// DataHub naming of one lineage namespace.
    /// </summary>
    public sealed class DataHubNamespaceOptions
    {
        /// <summary>
        /// DataHub platform id, null derives it from the namespace.
        /// </summary>
        public string? Platform { get; set; }

        /// <summary>
        /// Platform instance, prefixed to dataset names.
        /// </summary>
        public string? PlatformInstance { get; set; }

        /// <summary>
        /// Environment, null uses the store environment.
        /// </summary>
        public string? Env { get; set; }

        /// <summary>
        /// Database for one and two part names.
        /// </summary>
        public string? Database { get; set; }

        /// <summary>
        /// Schema for one part names.
        /// </summary>
        public string? DefaultSchema { get; set; }

        /// <summary>
        /// Lowercases dataset names.
        /// </summary>
        public bool LowercaseNames { get; set; }

        /// <summary>
        /// Lowercases column names.
        /// </summary>
        public bool LowercaseColumns { get; set; }

        /// <summary>
        /// Serves dataset status and schema, null uses the store setting.
        /// </summary>
        public bool? IncludeDatasetMetadata { get; set; }
    }
}
