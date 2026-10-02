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
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal sealed record DbtRegistration(string StreamName, StreamLineage Lineage);

    internal static class DbtScopeMerger
    {
        private sealed class SnapshotIdentities
        {
            public SnapshotIdentities(DbtRegistration registration)
            {
                Registration = registration;
            }

            public DbtRegistration Registration { get; }

            public Dictionary<string, DbtTableIdentity?> InputsByKey { get; } = new Dictionary<string, DbtTableIdentity?>(StringComparer.Ordinal);

            public Dictionary<(string, string), DbtTableIdentity?> InputsByName { get; } = new Dictionary<(string, string), DbtTableIdentity?>();

            public List<DbtTableIdentity?> Outputs { get; } = new List<DbtTableIdentity?>();
        }

        // Registrations arrive in (stream, substream) ordinal order.
        public static IReadOnlyDictionary<DbtTableIdentity, DbtMergedTable> Merge(IReadOnlyList<DbtRegistration> registrations, DbtRelationResolution resolution)
        {
            var tables = new Dictionary<DbtTableIdentity, DbtMergedTable>();
            var snapshots = new List<SnapshotIdentities>(registrations.Count);

            foreach (var registration in registrations)
            {
                var snapshot = new SnapshotIdentities(registration);
                foreach (var input in registration.Lineage.Inputs)
                {
                    var identity = resolution.Resolve(input.Namespace, input.TableName, input.NameParts);
                    snapshot.InputsByKey.TryAdd(input.Key, identity);
                    snapshot.InputsByName.TryAdd((input.Namespace, input.TableName), identity);
                    if (identity is DbtTableIdentity id)
                    {
                        var table = GetTable(tables, id);
                        table.Readers.Add(registration.StreamName);
                        AddColumns(table.ReadColumns, input);
                    }
                }
                foreach (var output in registration.Lineage.Outputs)
                {
                    var identity = resolution.Resolve(output.Namespace, output.TableName, output.NameParts);
                    snapshot.Outputs.Add(identity);
                    if (identity is DbtTableIdentity id)
                    {
                        var table = GetTable(tables, id);
                        table.IsWritten = true;
                        table.Writers.Add(registration.StreamName);
                        AddColumns(table.WrittenColumns, output);
                        // Every projected alias needs a column entry.
                        foreach (var key in output.ColumnLineage?.Fields.Keys.Order(StringComparer.Ordinal) ?? Enumerable.Empty<string>())
                        {
                            table.WrittenColumns.Add(new DbtColumn(key, AnyType.Instance, false));
                        }
                    }
                }
                snapshots.Add(snapshot);
            }

            // Fields a reader uses must exist upstream.
            foreach (var snapshot in snapshots)
            {
                foreach (var field in GetModelOutputs(snapshot).SelectMany(x => AllInputFields(x.Output)))
                {
                    if (ResolveField(snapshot, field, resolution) is DbtTableIdentity id)
                    {
                        GetTable(tables, id).ReferencedColumns.Add(new DbtColumn(field.Field, AnyType.Instance, false));
                    }
                }
            }

            foreach (var table in tables.Values)
            {
                table.FinishColumns();
            }

            // Refs need the final casing, so a second pass.
            foreach (var snapshot in snapshots)
            {
                var streamName = snapshot.Registration.StreamName;
                foreach (var (output, identity) in GetModelOutputs(snapshot))
                {
                    var model = tables[identity];
                    foreach (var column in output.PlanColumns)
                    {
                        model.AddOutputColumn(model.Columns.Canonical(column.Name));
                    }
                    if (output.ColumnLineage is ColumnLineage columnLineage)
                    {
                        var planNames = output.PlanColumns.Select(x => x.Name).ToHashSet(StringComparer.Ordinal);
                        foreach (var extra in columnLineage.Fields.Keys.Where(x => !planNames.Contains(x)).Order(StringComparer.Ordinal))
                        {
                            model.AddOutputColumn(model.Columns.Canonical(extra));
                        }
                        foreach (var field in columnLineage.Fields)
                        {
                            var refs = model.GetFieldRefs(model.Columns.Canonical(field.Key));
                            foreach (var inputField in field.Value.InputFields)
                            {
                                refs.Add(ToRef(snapshot, inputField, resolution, tables, streamName));
                            }
                        }
                        foreach (var inputField in columnLineage.Dataset)
                        {
                            model.Dataset.Add(ToRef(snapshot, inputField, resolution, tables, streamName));
                        }
                    }
                    foreach (var key in output.UpstreamInputKeys)
                    {
                        if (snapshot.InputsByKey.TryGetValue(key, out var upstream) && upstream is DbtTableIdentity upstreamId)
                        {
                            model.AddReachable(upstreamId);
                        }
                    }
                }
            }
            return tables;
        }

        // Every resolved upstream except the model itself.
        public static HashSet<DbtTableIdentity> GetDependencies(DbtMergedTable model)
        {
            var result = new HashSet<DbtTableIdentity>();
            var refs = model.Fields.Values.SelectMany(x => x.Refs).Concat(model.Dataset.Refs);
            foreach (var inputRef in refs)
            {
                if (inputRef.Identity is DbtTableIdentity id)
                {
                    result.Add(id);
                }
            }
            result.UnionWith(model.Reachable);
            result.Remove(model.Identity);
            return result;
        }

        // Back edges found in unique id order are dropped.
        public static void BreakCycles(IReadOnlyList<DbtModelNode> models)
        {
            var byId = models.ToDictionary(x => x.Name.UniqueId, StringComparer.Ordinal);
            var color = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var root in models.OrderBy(x => x.Name.UniqueId, StringComparer.Ordinal))
            {
                if (color.ContainsKey(root.Name.UniqueId))
                {
                    continue;
                }
                var stack = new Stack<(DbtModelNode Node, int Edge)>();
                color[root.Name.UniqueId] = 1;
                stack.Push((root, 0));
                while (stack.Count > 0)
                {
                    var (node, edge) = stack.Pop();
                    if (edge >= node.DependsOn.Count)
                    {
                        color[node.Name.UniqueId] = 2;
                        continue;
                    }
                    var target = node.DependsOn[edge];
                    if (!byId.TryGetValue(target, out var targetNode))
                    {
                        stack.Push((node, edge + 1));
                        continue;
                    }
                    if (!color.TryGetValue(target, out var targetColor))
                    {
                        stack.Push((node, edge + 1));
                        color[target] = 1;
                        stack.Push((targetNode, 0));
                    }
                    else if (targetColor == 1)
                    {
                        node.DependsOn.RemoveAt(edge);
                        node.DroppedDependencies.Add(target);
                        stack.Push((node, edge));
                    }
                    else
                    {
                        stack.Push((node, edge + 1));
                    }
                }
            }
        }

        private static IEnumerable<(StreamLineageOutput Output, DbtTableIdentity Identity)> GetModelOutputs(SnapshotIdentities snapshot)
        {
            var outputs = snapshot.Registration.Lineage.Outputs;
            for (int i = 0; i < outputs.Count; i++)
            {
                if (snapshot.Outputs[i] is DbtTableIdentity id)
                {
                    yield return (outputs[i], id);
                }
            }
        }

        private static IEnumerable<LineageInputField> AllInputFields(StreamLineageOutput output)
        {
            if (output.ColumnLineage == null)
            {
                return [];
            }
            return output.ColumnLineage.Fields.Values.SelectMany(x => x.InputFields).Concat(output.ColumnLineage.Dataset);
        }

        private static DbtTableIdentity? ResolveField(SnapshotIdentities snapshot, LineageInputField field, DbtRelationResolution resolution)
        {
            if (snapshot.InputsByName.TryGetValue((field.Namespace, field.TableName), out var identity))
            {
                return identity;
            }
            return resolution.Resolve(field.Namespace, field.TableName, field.TableName.Split('.'));
        }

        private static DbtInputRef ToRef(
            SnapshotIdentities snapshot,
            LineageInputField field,
            DbtRelationResolution resolution,
            Dictionary<DbtTableIdentity, DbtMergedTable> tables,
            string streamName)
        {
            var identity = ResolveField(snapshot, field, resolution);
            var name = field.Field;
            if (identity is DbtTableIdentity id)
            {
                var table = tables[id];
                table.Readers.Add(streamName);
                name = table.Columns.Canonical(name);
            }
            return new DbtInputRef(identity, field.Namespace, field.TableName, name, field.Transformations);
        }

        private static void AddColumns(List<DbtColumn> target, StreamLineageTable table)
        {
            if (table.ConnectorColumns != null)
            {
                foreach (var column in table.ConnectorColumns)
                {
                    target.Add(new DbtColumn(column.Name, column.Type, true));
                }
            }
            foreach (var column in table.PlanColumns)
            {
                target.Add(new DbtColumn(column.Name, column.Type, false));
            }
        }

        private static DbtMergedTable GetTable(Dictionary<DbtTableIdentity, DbtMergedTable> tables, DbtTableIdentity identity)
        {
            if (!tables.TryGetValue(identity, out var table))
            {
                table = new DbtMergedTable(identity);
                tables.Add(identity, table);
            }
            return table;
        }
    }
}
