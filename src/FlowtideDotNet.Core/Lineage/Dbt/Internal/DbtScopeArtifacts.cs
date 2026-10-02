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

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // Manifest and catalog always come from one project.
    internal sealed class DbtScopeArtifacts
    {
        private DbtScopeArtifacts(DbtArtifact manifest, DbtArtifact catalog)
        {
            Manifest = manifest;
            Catalog = catalog;
        }

        public DbtArtifact Manifest { get; }

        public DbtArtifact Catalog { get; }

        public static DbtScopeArtifacts Generate(IReadOnlyList<DbtRegistration> registrations, DbtGeneratorSettings settings)
        {
            var project = DbtProjectBuilder.Build(registrations, settings);
            // Placeholder pass, the id is a hash of the content.
            var draft = DbtManifestWriter.Write(project, Guid.Empty);
            var invocationId = DbtHashing.DeterministicUuid(draft);
            var manifest = DbtManifestWriter.Write(project, invocationId);
            var catalog = DbtCatalogWriter.Write(project, invocationId);
            return new DbtScopeArtifacts(
                new DbtArtifact(manifest, DbtHashing.ETag(manifest)),
                new DbtArtifact(catalog, DbtHashing.ETag(catalog)));
        }
    }
}
