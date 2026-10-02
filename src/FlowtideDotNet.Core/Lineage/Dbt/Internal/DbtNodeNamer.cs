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

using System.Text;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal sealed class DbtNodeName
    {
        public required string UniqueId { get; init; }

        public required string Name { get; init; }

        public required IReadOnlyList<string> Fqn { get; init; }

        public required string Path { get; init; }

        public required string OriginalFilePath { get; init; }

        // Only sources carry a source name.
        public string? SourceName { get; init; }
    }

    internal static class DbtNodeNamer
    {
        public static string Sanitize(string value)
        {
            if (value.Length == 0)
            {
                return "_";
            }
            var builder = new StringBuilder(value.Length);
            foreach (var c in value.ToLowerInvariant())
            {
                builder.Append((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' ? c : '_');
            }
            return builder.ToString();
        }

        public static IReadOnlyDictionary<DbtTableIdentity, DbtNodeName> Name(
            string projectName,
            IReadOnlyCollection<DbtTableIdentity> models,
            IReadOnlyCollection<DbtTableIdentity> sources)
        {
            var namespaces = models.Concat(sources).Select(x => x.Namespace).Distinct(StringComparer.Ordinal);
            var shortNames = Disambiguate(namespaces, ns => Sanitize(DbtRelationResolution.ShortNamespace(ns)), ns => [ns]);

            var result = new Dictionary<DbtTableIdentity, DbtNodeName>();

            var schemas = sources.Select(x => (x.Namespace, x.Database, x.Schema)).Distinct();
            var sourceNames = Disambiguate(
                schemas,
                s => Sanitize(JoinNonEmpty("_", shortNames[s.Namespace], s.Database, s.Schema)),
                s => [s.Namespace, s.Database ?? string.Empty, s.Schema]);
            foreach (var group in sources.GroupBy(x => sourceNames[(x.Namespace, x.Database, x.Schema)], StringComparer.Ordinal))
            {
                var tableParts = Disambiguate(group.Select(x => x.Identifier).Distinct(StringComparer.Ordinal), Sanitize, x => [x]);
                foreach (var source in group)
                {
                    result.Add(source, new DbtNodeName()
                    {
                        UniqueId = $"source.{projectName}.{group.Key}.{tableParts[source.Identifier]}",
                        Name = source.Identifier,
                        Fqn = [projectName, group.Key, source.Identifier],
                        Path = $"models/{projectName}_sources.yml",
                        OriginalFilePath = $"models/{projectName}_sources.yml",
                        SourceName = group.Key
                    });
                }
            }

            var modelNames = Disambiguate(
                models,
                m => JoinNonEmpty(
                    "__",
                    shortNames[m.Namespace],
                    m.Database == null ? null : Sanitize(m.Database),
                    m.Schema.Length == 0 ? null : Sanitize(m.Schema),
                    Sanitize(m.Identifier)),
                m => [m.Namespace, m.Database ?? string.Empty, m.Schema, m.Identifier]);
            foreach (var model in models)
            {
                var name = modelNames[model];
                var shortName = shortNames[model.Namespace];
                result.Add(model, new DbtNodeName()
                {
                    UniqueId = $"model.{projectName}.{name}",
                    Name = name,
                    Fqn = [projectName, shortName, name],
                    Path = $"{shortName}/{name}.sql",
                    OriginalFilePath = $"models/{shortName}/{name}.sql"
                });
            }
            return result;
        }

        // Colliding candidates all get a hash of their full identity.
        private static Dictionary<T, string> Disambiguate<T>(IEnumerable<T> items, Func<T, string> candidate, Func<T, string[]> identity)
            where T : notnull
        {
            var candidates = items.ToDictionary(x => x, candidate);
            var collisions = candidates
                .GroupBy(x => x.Value, StringComparer.Ordinal)
                .Where(x => x.Count() > 1)
                .Select(x => x.Key)
                .ToHashSet(StringComparer.Ordinal);

            var result = new Dictionary<T, string>(candidates.Count);
            foreach (var kv in candidates)
            {
                result.Add(kv.Key, collisions.Contains(kv.Value) ? kv.Value + "_" + DbtHashing.Hash8(identity(kv.Key)) : kv.Value);
            }
            return result;
        }

        private static string JoinNonEmpty(string separator, params string?[] parts)
        {
            return string.Join(separator, parts.Where(x => !string.IsNullOrEmpty(x)));
        }
    }
}
