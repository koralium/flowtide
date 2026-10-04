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

namespace FlowtideDotNet.Core.Lineage.DataHub
{
    /// <summary>
    /// Platform, name and environment of a DataHub dataset.
    /// </summary>
    public sealed class DataHubDataset
    {
        /// <summary>
        /// Creates a dataset identity.
        /// </summary>
        /// <param name="platform">DataHub platform id, such as mssql or kafka.</param>
        /// <param name="name">Dataset name without the platform instance.</param>
        /// <param name="platformInstance">Platform instance, null for none.</param>
        /// <param name="env">Environment, null uses the store environment.</param>
        public DataHubDataset(string platform, string name, string? platformInstance = null, string? env = null)
        {
            ArgumentException.ThrowIfNullOrEmpty(platform);
            ArgumentException.ThrowIfNullOrEmpty(name);
            Platform = platform;
            Name = name;
            PlatformInstance = string.IsNullOrEmpty(platformInstance) ? null : platformInstance;
            Env = string.IsNullOrEmpty(env) ? null : env;
        }

        /// <summary>
        /// DataHub platform id.
        /// </summary>
        public string Platform { get; }

        /// <summary>
        /// Dataset name without the platform instance.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Platform instance, null for none.
        /// </summary>
        public string? PlatformInstance { get; }

        /// <summary>
        /// Environment, null uses the store environment.
        /// </summary>
        public string? Env { get; }
    }
}
