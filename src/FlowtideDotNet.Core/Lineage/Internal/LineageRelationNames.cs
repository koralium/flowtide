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

namespace FlowtideDotNet.Core.Lineage.Internal
{
    // Splits a connector table name into database, schema and identifier.
    internal static class LineageRelationNames
    {
        public static string ShortNamespace(string @namespace)
        {
            var index = @namespace.IndexOf("://", StringComparison.Ordinal);
            return index < 0 ? @namespace : @namespace.Substring(0, index);
        }

        public static (string? Database, string Schema, string Identifier) Split(
            string shortNamespace,
            string tableName,
            IReadOnlyList<string> nameParts,
            string? mappedDatabase,
            string? mappedDefaultSchema)
        {
            var defaultSchema = mappedDefaultSchema ?? GetBuiltInDefaultSchema(shortNamespace) ?? string.Empty;
            if (IsFlat(shortNamespace))
            {
                return (mappedDatabase, defaultSchema, tableName);
            }

            var parts = nameParts.Count > 0 ? nameParts : [tableName];
            var count = parts.Count;
            return count switch
            {
                1 => (mappedDatabase, defaultSchema, parts[0]),
                2 => (mappedDatabase, parts[0], parts[1]),
                3 => (parts[0], parts[1], parts[2]),
                _ => (string.Join(".", parts.Take(count - 2)), parts[count - 2], parts[count - 1])
            };
        }

        // Topics and indexes keep dots inside one identifier.
        public static bool IsFlat(string shortNamespace)
        {
            return shortNamespace.Equals("elasticsearch", StringComparison.OrdinalIgnoreCase) ||
                shortNamespace.StartsWith("kafka", StringComparison.OrdinalIgnoreCase);
        }

        private static string? GetBuiltInDefaultSchema(string shortNamespace)
        {
            return shortNamespace.Equals("mssql", StringComparison.OrdinalIgnoreCase) ? "dbo" : null;
        }
    }
}
