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

using FlowtideDotNet.Core.Lineage.Internal;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal static class DbtProjectBuilder
    {
        public static DbtProject Build(IReadOnlyList<DbtRegistration> registrations, DbtGeneratorSettings settings)
        {
            var resolution = new DbtRelationResolution(settings);
            var tables = DbtScopeMerger.Merge(registrations, resolution);
            var dialect = settings.Dialect;

            var models = tables.Values.Where(x => x.IsWritten).ToList();
            var modelIds = models.Select(x => x.Identity).ToHashSet();
            // Sources exist only as some model's dependency.
            var sourceIds = new HashSet<DbtTableIdentity>();
            foreach (var model in models)
            {
                sourceIds.UnionWith(DbtScopeMerger.GetDependencies(model).Where(x => !modelIds.Contains(x)));
            }

            var names = DbtNodeNamer.Name(settings.ProjectName, modelIds, sourceIds);
            var collisions = FindRelationCollisions(names);

            var modelNodes = new List<DbtModelNode>(models.Count);
            foreach (var model in models)
            {
                var mockSql = DbtMockSqlGenerator.Generate(model, x => names[x].UniqueId, dialect);
                var dependsOn = mockSql.FromOrder.Select(x => names[x].UniqueId).ToList();
                var node = new DbtModelNode()
                {
                    Name = names[model.Identity],
                    Identity = model.Identity,
                    RelationName = dialect.QuoteRelation(model.Identity),
                    Columns = model.Columns.Columns.Select(x => new DbtNodeColumn()
                    {
                        Name = x.Name,
                        DataType = dialect.MapDataType(x.Type),
                        Inputs = model.Fields.TryGetValue(x.Name, out var refs) && refs.Refs.Count > 0 ? string.Join("; ", refs.Refs.Select(FormatRef)) : null
                    }).ToList(),
                    CompiledCode = mockSql.Sql,
                    Checksum = DbtHashing.Sha256Hex(mockSql.Sql),
                    Streams = string.Join(",", model.Writers),
                    DependsOn = dependsOn,
                    RelationCollision = collisions.GetValueOrDefault(model.Identity)
                };
                if (mockSql.SelfReference)
                {
                    node.DroppedDependencies.Add(node.Name.UniqueId);
                }
                modelNodes.Add(node);
            }

            DbtScopeMerger.BreakCycles(modelNodes);

            var nodesById = new Dictionary<string, DbtNodeName>(StringComparer.Ordinal);
            foreach (var name in names.Values)
            {
                nodesById.Add(name.UniqueId, name);
            }
            foreach (var node in modelNodes)
            {
                node.RefNames = node.DependsOn.Where(x => nodesById[x].SourceName == null).Select(x => nodesById[x].Name).ToList();
                node.Sources = node.DependsOn
                    .Where(x => nodesById[x].SourceName != null)
                    .Select(x => (nodesById[x].SourceName!, nodesById[x].Name))
                    .ToList();
            }

            var sourceNodes = sourceIds.Select(id => new DbtSourceNode()
            {
                Name = names[id],
                Identity = id,
                RelationName = dialect.QuoteRelation(id),
                Columns = tables[id].Columns.Columns.Select(x => new DbtNodeColumn()
                {
                    Name = x.Name,
                    DataType = dialect.MapDataType(x.Type)
                }).ToList(),
                Streams = string.Join(",", tables[id].Readers),
                RelationCollision = collisions.GetValueOrDefault(id)
            }).ToList();

            var parentMap = new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
            var childMap = new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
            foreach (var uniqueId in nodesById.Keys)
            {
                parentMap.Add(uniqueId, new List<string>());
                childMap.Add(uniqueId, new List<string>());
            }
            foreach (var node in modelNodes)
            {
                foreach (var dependency in node.DependsOn)
                {
                    parentMap[node.Name.UniqueId].Add(dependency);
                    childMap[dependency].Add(node.Name.UniqueId);
                }
            }
            foreach (var list in parentMap.Values.Concat(childMap.Values))
            {
                list.Sort(StringComparer.Ordinal);
            }

            return new DbtProject()
            {
                ProjectName = settings.ProjectName,
                AdapterType = dialect.AdapterType,
                GeneratedAt = registrations.Count == 0 ? DateTimeOffset.UnixEpoch : registrations.Max(x => x.Lineage.BuildTime),
                Models = modelNodes.OrderBy(x => x.Name.UniqueId, StringComparer.Ordinal).ToList(),
                Sources = sourceNodes.OrderBy(x => x.Name.UniqueId, StringComparer.Ordinal).ToList(),
                ParentMap = parentMap,
                ChildMap = childMap
            };
        }

        // Catalogs key nodes by relation, namespace is invisible.
        private static Dictionary<DbtTableIdentity, string> FindRelationCollisions(IReadOnlyDictionary<DbtTableIdentity, DbtNodeName> names)
        {
            var result = new Dictionary<DbtTableIdentity, string>();
            var groups = names.Keys
                .GroupBy(x => ((x.Database ?? string.Empty).ToUpperInvariant(), x.Schema.ToUpperInvariant(), x.Identifier.ToUpperInvariant()))
                .Where(x => x.Count() > 1);
            foreach (var group in groups)
            {
                foreach (var identity in group)
                {
                    var others = group
                        .Where(x => x != identity)
                        .Select(x => names[x].UniqueId)
                        .OrderBy(x => x, StringComparer.Ordinal);
                    result.Add(identity, string.Join(",", others));
                }
            }
            return result;
        }

        // One entry per input, ns:relation.field TYPE/SUBTYPE.
        private static string FormatRef(DbtInputRef inputRef)
        {
            var table = inputRef.Identity is DbtTableIdentity id
                ? DbtRelationResolution.ShortNamespace(id.Namespace) + ":" + string.Join(".", id.NonEmptyParts())
                : inputRef.Namespace + ":" + inputRef.TableName;
            var transformations = string.Join(",", inputRef.Transformations.Select(FormatTransformation));
            return transformations.Length == 0 ? $"{table}.{inputRef.Field}" : $"{table}.{inputRef.Field} {transformations}";
        }

        private static string FormatTransformation(LineageTransformation transformation)
        {
            var type = transformation.Type == LineageTransformationType.Direct ? "DIRECT" : "INDIRECT";
            // Explicit names, enum ToString would give GROUPBY.
            var subtype = transformation.SubType switch
            {
                LineageTransformationSubtype.Identity => "IDENTITY",
                LineageTransformationSubtype.Transformation => "TRANSFORMATION",
                LineageTransformationSubtype.Aggregation => "AGGREGATION",
                LineageTransformationSubtype.Join => "JOIN",
                LineageTransformationSubtype.GroupBy => "GROUP_BY",
                LineageTransformationSubtype.Filter => "FILTER",
                LineageTransformationSubtype.Sort => "SORT",
                LineageTransformationSubtype.Window => "WINDOW",
                LineageTransformationSubtype.Conditional => "CONDITIONAL",
                _ => "UNKNOWN"
            };
            return transformation.Masking ? $"{type}/{subtype}:masking" : $"{type}/{subtype}";
        }
    }
}
