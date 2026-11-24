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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    // Checks at build where row references may go
    internal sealed class RowReferencePlanValidator : OptimizerBaseVisitor
    {
        // The first reference column an input gave
        private sealed class Frame
        {
            public string? StoreName { get; set; }

            public int Column { get; set; }
        }

        private readonly IReadOnlyList<Relation> _relations;
        private readonly Dictionary<string, Relation> _hosts;
        // The store behind each output column, null for a value
        private readonly Dictionary<Relation, string?[]> _columns = new Dictionary<Relation, string?[]>(ReferenceEqualityComparer.Instance);
        private readonly HashSet<Relation> _visiting = new HashSet<Relation>(ReferenceEqualityComparer.Instance);
        private readonly List<Frame> _stack = new List<Frame>();

        private RowReferencePlanValidator(IReadOnlyList<Relation> relations, Dictionary<string, Relation> hosts)
        {
            _relations = relations;
            _hosts = hosts;
        }

        // Throws when a reference goes where it cannot be taken
        public static void Validate(IReadOnlyList<Relation> relations)
        {
            var validator = new RowReferencePlanValidator(relations, RowStoreHostCollector.Collect(relations));
            for (int i = 0; i < relations.Count; i++)
            {
                validator.Visit(relations[i], null!);
            }
        }

        public override Relation Visit(Relation relation, object state)
        {
            if (!_columns.TryGetValue(relation, out var columns))
            {
                if (!_visiting.Add(relation))
                {
                    // A loop reads its own output, checked where it is built
                    return relation;
                }
                _stack.Add(new Frame());
                base.Visit(relation, state);
                var frame = _stack[_stack.Count - 1];
                _stack.RemoveAt(_stack.Count - 1);
                columns = Columns(relation, frame);
                _columns.Add(relation, columns);
                _visiting.Remove(relation);
            }
            if (_stack.Count > 0)
            {
                var parent = _stack[_stack.Count - 1];
                for (int i = 0; i < columns.Length && parent.StoreName == null; i++)
                {
                    if (columns[i] != null)
                    {
                        parent.StoreName = columns[i];
                        parent.Column = i;
                    }
                }
            }
            return relation;
        }

        private string?[] Columns(Relation relation, Frame inputs)
        {
            switch (relation)
            {
                case ReadRelation read:
                    return HostColumns(read, read.RowStore, read.OutputLengthWithoutRowStore);
                case NormalizationRelation normalization:
                    RefuseReferences(relation, inputs);
                    return HostColumns(normalization, normalization.RowStore, normalization.OutputLengthWithoutRowStore);
                case FetchRequiredColumnsRelation fetch:
                    return FetchColumns(fetch);
                case ReferenceRelation reference:
                    return ReferenceColumns(reference);
                case RootRelation root:
                    return _columns.TryGetValue(root.Input, out var rootInput) ? rootInput : new string?[root.Input.OutputLength];
                default:
                    RefuseReferences(relation, inputs);
                    return new string?[relation.OutputLength];
            }
        }

        private static void RefuseReferences(Relation relation, Frame inputs)
        {
            if (inputs.StoreName != null)
            {
                throw new InvalidOperationException(
                    $"A {Name(relation)} gets row references of row store '{inputs.StoreName}' in column {inputs.Column} of an input, it cannot take them. Read the stored columns with a fetch before it.");
            }
        }

        private static string Name(Relation relation)
        {
            var name = relation.GetType().Name;
            return name.EndsWith("Relation") ? name.Substring(0, name.Length - "Relation".Length) + " relation" : name;
        }

        private static string?[] HostColumns(Relation relation, RowStoreDefinition? rowStore, int lengthWithoutStore)
        {
            if (rowStore == null)
            {
                return new string?[relation.OutputLength];
            }
            if (string.IsNullOrEmpty(rowStore.Name))
            {
                throw new InvalidOperationException($"A {Name(relation)} has a row store without a name.");
            }
            if (rowStore.StoredColumns.Count == 0)
            {
                throw new InvalidOperationException($"Row store '{rowStore.Name}' stores no column.");
            }
            for (int i = 0; i < rowStore.StoredColumns.Count; i++)
            {
                int position = rowStore.StoredColumns[i];
                if (position < 0 || position >= lengthWithoutStore)
                {
                    throw new InvalidOperationException($"Row store '{rowStore.Name}' stores output column {position}, the relation has {lengthWithoutStore}.");
                }
                if (rowStore.StoredColumns.IndexOf(position) != i)
                {
                    throw new InvalidOperationException($"Row store '{rowStore.Name}' names output column {position} twice.");
                }
            }
            var columns = new string?[relation.OutputLength];
            columns[columns.Length - 1] = rowStore.Name;
            return columns;
        }

        private string?[] FetchColumns(FetchRequiredColumnsRelation fetch)
        {
            var input = _columns.TryGetValue(fetch.Input, out var known) ? known : new string?[fetch.Input.OutputLength];
            int length = input.Length;
            for (int i = 0; i < fetch.Fetches.Count; i++)
            {
                for (int k = 0; k < i; k++)
                {
                    if (fetch.Fetches[k].ReferenceColumn == fetch.Fetches[i].ReferenceColumn)
                    {
                        throw new InvalidOperationException($"Two fetches read through reference column {fetch.Fetches[i].ReferenceColumn}, name every column of a store in one fetch.");
                    }
                }
            }
            foreach (var store in fetch.Fetches)
            {
                if (!_hosts.TryGetValue(store.StoreName, out var host))
                {
                    throw new InvalidOperationException($"A fetch reads from row store '{store.StoreName}', no relation of the plan hosts it.");
                }
                if (store.ReferenceColumn < 0 || store.ReferenceColumn >= input.Length)
                {
                    throw new InvalidOperationException($"A fetch from row store '{store.StoreName}' reads its references from column {store.ReferenceColumn}, the input has {input.Length}.");
                }
                if (input[store.ReferenceColumn] == null)
                {
                    throw new InvalidOperationException($"A fetch from row store '{store.StoreName}' reads its references from column {store.ReferenceColumn}, that column holds no row references.");
                }
                if (input[store.ReferenceColumn] != store.StoreName)
                {
                    throw new InvalidOperationException($"A fetch from row store '{store.StoreName}' reads its references from column {store.ReferenceColumn}, that column holds references of row store '{input[store.ReferenceColumn]}'.");
                }
                if (store.Columns.Count == 0)
                {
                    throw new InvalidOperationException($"A fetch from row store '{store.StoreName}' reads no column.");
                }
                int stored = RowStoreHostCollector.RowStoreOf(host)!.StoredColumns.Count;
                foreach (var column in store.Columns)
                {
                    if (column < 0 || column >= stored)
                    {
                        throw new InvalidOperationException($"A fetch asks for stored column {column} of row store '{store.StoreName}', it stores {stored}.");
                    }
                }
                length += store.Columns.Count;
            }
            // Fetched columns hold values
            var columns = new string?[length];
            Array.Copy(input, columns, input.Length);
            return Emitted(fetch, columns);
        }

        private string?[] ReferenceColumns(ReferenceRelation reference)
        {
            if (reference.RelationId < 0 || reference.RelationId >= _relations.Count)
            {
                return new string?[reference.OutputLength];
            }
            var target = _relations[reference.RelationId];
            if (!_columns.TryGetValue(target, out var columns))
            {
                if (_visiting.Contains(target))
                {
                    return new string?[reference.OutputLength];
                }
                // Visited as a root, not as this relation's input
                var stack = new List<Frame>(_stack);
                _stack.Clear();
                Visit(target, null!);
                _stack.AddRange(stack);
                columns = _columns[target];
            }
            bool references = false;
            for (int i = 0; i < columns.Length; i++)
            {
                references |= columns[i] != null;
            }
            if (!references)
            {
                return new string?[reference.OutputLength];
            }
            // At run time the target's output is linked as it is
            if (reference.EmitSet)
            {
                throw new InvalidOperationException($"A reference to relation {reference.RelationId} has an emit and that relation gives row references, the emit is not applied when the stream runs.");
            }
            if (reference.ReferenceOutputLength != columns.Length)
            {
                throw new InvalidOperationException($"A reference to relation {reference.RelationId} says it has {reference.ReferenceOutputLength} columns, that relation gives {columns.Length} with its row references.");
            }
            return columns;
        }

        private static string?[] Emitted(Relation relation, string?[] columns)
        {
            if (!relation.EmitSet)
            {
                return columns;
            }
            var emitted = new string?[relation.Emit.Count];
            for (int i = 0; i < emitted.Length; i++)
            {
                int column = relation.Emit[i];
                if (column < 0 || column >= columns.Length)
                {
                    throw new InvalidOperationException($"A {Name(relation)} emits column {column}, it has {columns.Length}.");
                }
                emitted[i] = columns[column];
            }
            return emitted;
        }
    }
}
