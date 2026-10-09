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

using System.Reflection;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal static class OpenLineageConstants
    {
        public const string Producer = "https://github.com/koralium/flowtide";

        public const string RunEventSchemaUrl = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

        public const string ProcessingEngineRunFacetSchemaUrl = "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet";

        public const string JobTypeJobFacetSchemaUrl = "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet";

        public const string ColumnLineageDatasetFacetSchemaUrl = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet";

        public const string SchemaDatasetFacetSchemaUrl = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet";

        public const string EngineName = "Flowtide";

        public const string JobNamespace = "flowtide";

        public const string Integration = "flowtide";

        // Core assembly, never the entry assembly.
        public static readonly string EngineVersion = ResolveEngineVersion(
            typeof(OpenLineageConstants).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
            typeof(OpenLineageConstants).Assembly.GetName().Version);

        internal static string ResolveEngineVersion(string? informationalVersion, Version? assemblyVersion)
        {
            if (!string.IsNullOrEmpty(informationalVersion))
            {
                // Strip the "+sha" source revision suffix.
                var plusIndex = informationalVersion.IndexOf('+');
                var version = plusIndex < 0 ? informationalVersion : informationalVersion.Substring(0, plusIndex);
                if (version.Length > 0)
                {
                    return version;
                }
            }
            if (assemblyVersion == null)
            {
                return "unknown";
            }
            return assemblyVersion.Build < 0 ? assemblyVersion.ToString() : assemblyVersion.ToString(3);
        }
    }
}
