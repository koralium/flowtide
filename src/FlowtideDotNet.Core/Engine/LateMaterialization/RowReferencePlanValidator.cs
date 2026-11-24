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

using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Expressions;
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
        private readonly IFunctionsRegister _functions;
        private readonly int _parallelism;
        // The store behind each output column, null for a value
        private readonly Dictionary<Relation, string?[]> _columns = new Dictionary<Relation, string?[]>(ReferenceEqualityComparer.Instance);
        private readonly HashSet<Relation> _visiting = new HashSet<Relation>(ReferenceEqualityComparer.Instance);
        private readonly List<Frame> _stack = new List<Frame>();

        private RowReferencePlanValidator(IReadOnlyList<Relation> relations, Dictionary<string, Relation> hosts, IFunctionsRegister functions, int parallelism)
        {
            _relations = relations;
            _hosts = hosts;
            _functions = functions;
            _parallelism = parallelism;
        }

        // Throws when a reference goes where it cannot be taken
        public static void Validate(IReadOnlyList<Relation> relations, IFunctionsRegister? functions = null, int parallelism = 1, bool columnStore = true)
        {
            var hosts = RowStoreHostCollector.Collect(relations);
            if (hosts.Count > 0 && !columnStore)
            {
                throw new InvalidOperationException("A plan with a row store needs the column store, the row operators cannot take row references.");
            }
            if (functions == null)
            {
                // Left out, the built-in functions are assumed
                var builtIn = new FunctionsRegister();
                BuiltinFunctions.RegisterFunctions(builtIn);
                functions = builtIn;
            }
            var validator = new RowReferencePlanValidator(relations, hosts, functions, parallelism);
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
                case FilterRelation filter:
                    return FilterColumns(filter);
                case ProjectRelation project:
                    return ProjectColumns(project);
                case MergeJoinRelation join:
                    return JoinColumns(join);
                case ExchangeRelation exchange:
                    return ExchangeColumns(exchange);
                case StandardOutputExchangeReferenceRelation exchangeOutput:
                    return ExchangeOutputColumns(exchangeOutput);
                case SetRelation set when set.Operation == SetOperation.UnionAll:
                    return UnionColumns(set);
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

        private string?[] Input(Relation input)
        {
            return _columns.TryGetValue(input, out var known) ? known : new string?[input.OutputLength];
        }

        private static bool HasReferences(string?[] columns)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                if (columns[i] != null)
                {
                    return true;
                }
            }
            return false;
        }

        // A carrier passes references on and never reads them
        private string?[] FilterColumns(FilterRelation filter)
        {
            var input = Input(filter.Input);
            if (!HasReferences(input))
            {
                return new string?[filter.OutputLength];
            }
            RowReferenceExpressionCheck.Check(filter, "condition", filter.Condition, input, _functions);
            return Emitted(filter, input);
        }

        private string?[] ProjectColumns(ProjectRelation project)
        {
            var input = Input(project.Input);
            if (!HasReferences(input))
            {
                return new string?[project.OutputLength];
            }
            var columns = new string?[input.Length + project.Expressions.Count];
            Array.Copy(input, columns, input.Length);
            for (int i = 0; i < project.Expressions.Count; i++)
            {
                // A plain field reference gives the reference on
                if (project.Expressions[i] is DirectFieldReference direct &&
                    direct.ReferenceSegment is StructReferenceSegment segment &&
                    segment.Child == null &&
                    segment.Field >= 0 && segment.Field < input.Length)
                {
                    columns[input.Length + i] = input[segment.Field];
                }
                else
                {
                    RowReferenceExpressionCheck.Check(project, "expressions", project.Expressions[i], input, _functions);
                }
            }
            return Emitted(project, columns);
        }

        private string?[] JoinColumns(MergeJoinRelation join)
        {
            var left = Input(join.Left);
            var right = Input(join.Right);
            if (!HasReferences(left) && !HasReferences(right))
            {
                return new string?[join.OutputLength];
            }
            if (join.MultiJoinGroup == null && _parallelism > 1)
            {
                throw new InvalidOperationException(
                    "A merge join gets row references and the stream runs with parallelism set on the builder, that partitions rows as row events and a row reference cannot be one. Fetch the stored columns before the join, or parallelize in the plan with exchanges.");
            }
            var both = new string?[left.Length + right.Length];
            Array.Copy(left, both, left.Length);
            Array.Copy(right, 0, both, left.Length, right.Length);
            for (int i = 0; i < join.LeftKeys.Count; i++)
            {
                RowReferenceExpressionCheck.Check(join, "keys", join.LeftKeys[i], both, _functions);
            }
            for (int i = 0; i < join.RightKeys.Count; i++)
            {
                RowReferenceExpressionCheck.Check(join, "keys", join.RightKeys[i], both, _functions);
            }
            if (join.PostJoinFilter != null)
            {
                RowReferenceExpressionCheck.Check(join, "condition", join.PostJoinFilter, both, _functions);
            }
            if (join.Type == JoinType.LeftMark)
            {
                // The mark is a value
                var marked = new string?[left.Length + 1];
                Array.Copy(left, marked, left.Length);
                return Emitted(join, marked);
            }
            return Emitted(join, both);
        }

        private string?[] ExchangeColumns(ExchangeRelation exchange)
        {
            var input = Input(exchange.Input);
            if (!HasReferences(input))
            {
                return new string?[exchange.OutputLength];
            }
            for (int i = 0; i < exchange.Targets.Count; i++)
            {
                if (exchange.Targets[i] is not StandardOutputExchangeTarget)
                {
                    throw new InvalidOperationException(
                        $"An exchange gets row references of row store '{First(input, out var column)}' in column {column} and sends them out of the stream, a reference is only good next to its store. Fetch the stored columns before it.");
                }
            }
            if (exchange.ExchangeKind is ScatterExchangeKind scatter)
            {
                // Rows are routed on values, never on a reference
                for (int i = 0; i < scatter.Fields.Count; i++)
                {
                    RowReferenceExpressionCheck.Check(exchange, "scatter fields", scatter.Fields[i], input, _functions);
                }
            }
            else if (exchange.ExchangeKind is not BroadcastExchangeKind)
            {
                throw new InvalidOperationException(
                    $"An exchange of kind {exchange.ExchangeKind.Type} gets row references of row store '{First(input, out var column)}' in column {column}, only scatter and broadcast exchanges pass them on.");
            }
            // At run time an exchange sends its input as it is
            if (exchange.EmitSet)
            {
                throw new InvalidOperationException(
                    $"An exchange gets row references of row store '{First(input, out var column)}' in column {column} and has an emit, the emit is not applied when the stream runs.");
            }
            return input;
        }

        private string?[] ExchangeOutputColumns(StandardOutputExchangeReferenceRelation output)
        {
            if (output.RelationId < 0 || output.RelationId >= _relations.Count)
            {
                return new string?[output.OutputLength];
            }
            var columns = RootColumns(_relations[output.RelationId]);
            if (columns == null || !HasReferences(columns))
            {
                return new string?[output.OutputLength];
            }
            // At run time the exchange's output is linked as it is
            if (output.EmitSet)
            {
                throw new InvalidOperationException($"A reference to exchange {output.RelationId} has an emit and that exchange gives row references, the emit is not applied when the stream runs.");
            }
            if (output.ReferenceOutputLength != columns.Length)
            {
                throw new InvalidOperationException($"A reference to exchange {output.RelationId} says it has {output.ReferenceOutputLength} columns, the exchange gives {columns.Length} with its row references.");
            }
            return columns;
        }

        private string?[] UnionColumns(SetRelation set)
        {
            var first = Input(set.Inputs[0]);
            bool references = HasReferences(first);
            for (int i = 1; i < set.Inputs.Count; i++)
            {
                references |= HasReferences(Input(set.Inputs[i]));
            }
            if (!references)
            {
                return new string?[set.OutputLength];
            }
            for (int i = 1; i < set.Inputs.Count; i++)
            {
                var other = Input(set.Inputs[i]);
                for (int c = 0; c < first.Length || c < other.Length; c++)
                {
                    var here = c < first.Length ? first[c] : null;
                    var there = c < other.Length ? other[c] : null;
                    if (here != there)
                    {
                        throw new InvalidOperationException(
                            $"A union gets {Describe(here)} in column {c} of input 0 and {Describe(there)} in column {c} of input {i}, one fetch must be able to read the column. Fetch the stored columns before the union.");
                    }
                }
            }
            return Emitted(set, first);
        }

        private static string Describe(string? store)
        {
            return store == null ? "values" : $"row references of row store '{store}'";
        }

        private static string First(string?[] columns, out int column)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                if (columns[i] != null)
                {
                    column = i;
                    return columns[i]!;
                }
            }
            column = -1;
            return "";
        }

        // Columns of a numbered relation, null inside a loop
        private string?[]? RootColumns(Relation target)
        {
            if (!_columns.TryGetValue(target, out var columns))
            {
                if (_visiting.Contains(target))
                {
                    return null;
                }
                // Visited as a root, not as this relation's input
                var stack = new List<Frame>(_stack);
                _stack.Clear();
                Visit(target, null!);
                _stack.AddRange(stack);
                columns = _columns[target];
            }
            return columns;
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
            var input = Input(fetch.Input);
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
            var columns = RootColumns(_relations[reference.RelationId]);
            if (columns == null || !HasReferences(columns))
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
