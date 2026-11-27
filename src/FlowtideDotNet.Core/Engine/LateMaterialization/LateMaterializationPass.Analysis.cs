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
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    internal sealed partial class LateMaterializationPass
    {
        // The constants of the cost model, bytes per row
        internal static class Cost
        {
            // A row reference, and the least a column counts
            public const int Reference = 8;
            // A column whose type is not known
            public const int UnknownWidth = 16;
            // Strings and binary, the values are not known
            public const int VariableWidth = 32;
            public const int NestedWidth = 48;
            // One read of a row through its reference
            public const int Fetch = 16;
            // A keyed host keeps a hash next to the reference
            public const int Hash = 8;
            // A keyed host reads the heap back when hashes meet
            public const int Verification = 8;
            // A store must save this much to be placed
            public const int Threshold = 128;
        }

        // Where a column's values come from, counted along every path
        private sealed class Origin
        {
            public required Relation Producer { get; init; }
            public required int Position { get; init; }
            public required bool Host { get; init; }
            public required int Width { get; init; }
            // The host's own column behind this output position
            public int Column { get; init; } = -1;
            // Places keeping a copy before a reader, by row share
            public Dictionary<Relation, double> Sites { get; } = new Dictionary<Relation, double>(ReferenceEqualityComparer.Instance);
            public double Copies => Sites.Values.Sum();
            // The relations that read it back, by row share
            public Dictionary<Relation, double> Readers { get; } = new Dictionary<Relation, double>(ReferenceEqualityComparer.Instance);
        }

        // Reads the plan and changes nothing
        private sealed class Analysis : Optimizer.OptimizerBaseVisitor
        {
            private readonly List<Relation> _relations;
            private readonly IConnectorManager? _connectorManager;
            private readonly IFunctionsRegister _functions;
            private readonly int _parallelism;
            private readonly HashSet<int> _referenced;
            private readonly HashSet<int> _exactRoots;
            private readonly Dictionary<Relation, Origin?[]> _origins = new Dictionary<Relation, Origin?[]>(ReferenceEqualityComparer.Instance);
            // A relation's part of the rows, less under a scatter
            private readonly Dictionary<Relation, double> _shares = new Dictionary<Relation, double>(ReferenceEqualityComparer.Instance);
            private readonly int[] _rootState;
            // Stands for a sink's key fetch, one per write
            private readonly Dictionary<Relation, Relation> _keyFetches = new Dictionary<Relation, Relation>(ReferenceEqualityComparer.Instance);
            // The relation a stand-in fetch sits on
            private readonly Dictionary<Relation, Relation> _standsFor = new Dictionary<Relation, Relation>(ReferenceEqualityComparer.Instance);
            // Stands for the fetch between an input and its join
            private readonly Dictionary<Relation, Relation> _sideFetches = new Dictionary<Relation, Relation>(ReferenceEqualityComparer.Instance);

            // The input may read stored columns itself, under its own fetch
            private Relation SideFetch(Relation input)
            {
                if (!_sideFetches.TryGetValue(input, out var fetch))
                {
                    fetch = new ReferenceRelation();
                    _sideFetches.Add(input, fetch);
                    _standsFor.Add(fetch, input);
                }
                return fetch;
            }
            // Inputs of the relation being read, innermost last
            private readonly List<List<Relation>> _inputs = new List<List<Relation>>();
            private int _loopDepth;
            private bool _underNormalization;

            public Analysis(List<Relation> relations, IConnectorManager? connectorManager, IFunctionsRegister functions, int parallelism, ReferenceScan scan)
            {
                _relations = relations;
                _connectorManager = connectorManager;
                _functions = functions;
                _parallelism = parallelism;
                _referenced = scan.Referenced;
                _exactRoots = scan.Exact;
                _rootState = new int[relations.Count];
            }

            // Every origin made, in plan order
            public List<Origin> All { get; } = new List<Origin>();

            // Why the plan cannot be rewritten safely, if so
            public string? Mismatch { get; private set; }

            // Hosts in plan order: table, and if the heap adds a copy
            public List<(Relation Host, string Table, bool AddsCopy)> Hosts { get; } = new List<(Relation, string, bool)>();

            public void Root(int index)
            {
                if (_rootState[index] != 0)
                {
                    return;
                }
                _rootState[index] = 1;
                int loopDepth = _loopDepth;
                bool underNormalization = _underNormalization;
                _loopDepth = 0;
                _underNormalization = false;
                Visit(_relations[index], null!);
                if (!_referenced.Contains(index) || _exactRoots.Contains(index))
                {
                    // Nothing takes references from this relation
                    Read(_origins[_relations[index]], _relations[index], _relations[index]);
                }
                _loopDepth = loopDepth;
                _underNormalization = underNormalization;
                _rootState[index] = 2;
            }

            private Origin Make(Relation producer, int position, bool host, int width, int column = -1)
            {
                var origin = new Origin() { Producer = producer, Position = position, Host = host, Width = width, Column = column };
                All.Add(origin);
                return origin;
            }

            // A fresh origin per column: values a later store could defer
            private Origin?[] Fresh(Relation relation, int length)
            {
                var origins = new Origin?[length];
                for (int i = 0; i < length && _loopDepth == 0; i++)
                {
                    origins[i] = Make(relation, i, false, Cost.UnknownWidth);
                }
                return origins;
            }

            private double Share(Relation relation)
            {
                return _shares.TryGetValue(relation, out var share) ? share : 1;
            }

            // Read here: a deferred column is fetched in front
            private Origin? Read(Origin? origin, Relation reader, int position, double share)
            {
                if (origin == null)
                {
                    return null;
                }
                if (origin.Host)
                {
                    origin.Readers[reader] = origin.Readers.TryGetValue(reader, out var before) ? before + share : share;
                }
                // A stand-in is no place in the plan, its input is
                var producer = _standsFor.TryGetValue(reader, out var place) ? place : reader;
                return _loopDepth == 0 ? Make(producer, position, false, origin.Width) : null;
            }

            // Every column of the input is read by the reader
            private void Read(Origin?[] origins, Relation reader, Relation input)
            {
                double share = Share(input);
                var seen = new HashSet<Origin>(ReferenceEqualityComparer.Instance);
                for (int i = 0; i < origins.Length; i++)
                {
                    var origin = origins[i];
                    if (origin != null && !seen.Add(origin))
                    {
                        // A second name of one column is one read
                        origins[i] = Make(_standsFor.TryGetValue(reader, out var place) ? place : reader, i, false, origin.Width);
                        continue;
                    }
                    origins[i] = Read(origin, reader, i, share);
                }
            }

            private void Read(Origin?[] origins, bool[] need, Relation reader, double share)
            {
                var read = new Dictionary<Origin, Origin?>(ReferenceEqualityComparer.Instance);
                for (int i = 0; i < origins.Length; i++)
                {
                    var origin = origins[i];
                    if (origin == null || !need[i])
                    {
                        continue;
                    }
                    if (!read.TryGetValue(origin, out var after))
                    {
                        after = Read(origin, reader, i, share);
                        read.Add(origin, after);
                    }
                    origins[i] = after;
                }
                // Every name of a read column holds the value
                for (int i = 0; i < origins.Length; i++)
                {
                    if (origins[i] != null && read.TryGetValue(origins[i]!, out var after))
                    {
                        origins[i] = after;
                    }
                }
            }

            // The input keeps one copy of every column carried past it
            private static void Stored(Origin?[] origins, double share, Relation input, Origin?[] before)
            {
                for (int i = 0; i < origins.Length; i++)
                {
                    // What the relation read itself is a value it keeps anyway.
                    // A second name of a column is the same copy.
                    if (ReferenceEquals(origins[i], before[i]))
                    {
                        origins[i]?.Sites.TryAdd(input, share);
                    }
                }
            }

            private static Origin?[] Emitted(Relation relation, Origin?[] origins)
            {
                if (!relation.EmitSet)
                {
                    return origins;
                }
                var emitted = new Origin?[relation.Emit.Count];
                for (int i = 0; i < emitted.Length; i++)
                {
                    emitted[i] = origins[relation.Emit[i]];
                }
                return emitted;
            }

            private Origin?[] Of(Relation relation)
            {
                return (Origin?[])_origins[relation].Clone();
            }

            public override Relation Visit(Relation relation, object state)
            {
                if (_inputs.Count > 0)
                {
                    _inputs[_inputs.Count - 1].Add(relation);
                }
                _inputs.Add(new List<Relation>());
                Origin?[] origins;
                switch (relation)
                {
                    case ReadRelation read:
                        origins = ReadOrigins(read);
                        break;
                    case NormalizationRelation normalization:
                        origins = NormalizationOrigins(normalization);
                        break;
                    case FilterRelation filter:
                        origins = FilterOrigins(filter, null);
                        break;
                    case ProjectRelation project:
                        origins = ProjectOrigins(project);
                        break;
                    case MergeJoinRelation join:
                        origins = JoinOrigins(join);
                        break;
                    case ExchangeRelation exchange:
                        origins = ExchangeOrigins(exchange);
                        break;
                    case ReferenceRelation reference:
                        origins = Target(reference.RelationId, reference);
                        break;
                    case StandardOutputExchangeReferenceRelation output:
                        origins = Target(output.RelationId, output);
                        break;
                    case SetRelation set when set.Operation == SetOperation.UnionAll:
                        origins = UnionOrigins(set);
                        break;
                    case WriteRelation write:
                        origins = WriteOrigins(write);
                        break;
                    default:
                        origins = OtherOrigins(relation);
                        break;
                }
                var inputs = _inputs[_inputs.Count - 1];
                _inputs.RemoveAt(_inputs.Count - 1);
                _origins[relation] = origins;
                _shares[relation] = ShareOf(relation, inputs);
                return relation;
            }

            // A scatter sends each row to one of its targets
            private double ShareOf(Relation relation, List<Relation> inputs)
            {
                if (relation is StandardOutputExchangeReferenceRelation output && output.RelationId >= 0 && output.RelationId < _relations.Count)
                {
                    var target = _relations[output.RelationId];
                    if (target is ExchangeRelation exchange && exchange.ExchangeKind is ScatterExchangeKind && exchange.Targets.Count > 0)
                    {
                        // A target without partitions of its own gets every row
                        int partitions = exchange.PartitionCount ?? exchange.Targets.Count;
                        int mine = output.TargetId >= 0 && output.TargetId < exchange.Targets.Count ? exchange.Targets[output.TargetId].PartitionIds.Count : 0;
                        return Share(target) * (mine == 0 || partitions <= 0 ? 1 : Math.Min(1, (double)mine / partitions));
                    }
                    return Share(target);
                }
                if (relation is ReferenceRelation reference && reference.RelationId >= 0 && reference.RelationId < _relations.Count)
                {
                    return Share(_relations[reference.RelationId]);
                }
                if (inputs.Count == 0)
                {
                    return 1;
                }
                if (relation is SetRelation set && set.Operation == SetOperation.UnionAll)
                {
                    // The partitions come together again
                    return Math.Min(1, inputs.Sum(Share));
                }
                // A join's rows follow its smallest input's part
                return inputs.Min(Share);
            }

            // Any other relation reads whole rows from every input
            private Origin?[] OtherOrigins(Relation relation)
            {
                bool loop = relation is IterationRelation;
                if (loop)
                {
                    _loopDepth++;
                }
                relation.Accept(this, null!);
                foreach (var input in _inputs[_inputs.Count - 1])
                {
                    Read(_origins[input], relation, input);
                }
                if (loop)
                {
                    _loopDepth--;
                }
                return Fresh(relation, relation.OutputLength);
            }

            private static int Width(SubstraitBaseType? type)
            {
                switch (type?.Type)
                {
                    case SubstraitType.Bool:
                    case SubstraitType.Int32:
                    case SubstraitType.Int64:
                    case SubstraitType.Fp32:
                    case SubstraitType.Fp64:
                    case SubstraitType.Date:
                    case SubstraitType.TimestampTz:
                    case SubstraitType.Null:
                        // No column counts below a reference
                        return Cost.Reference;
                    case SubstraitType.Decimal:
                        return 16;
                    case SubstraitType.String:
                    case SubstraitType.Binary:
                        return Cost.VariableWidth;
                    case SubstraitType.List:
                    case SubstraitType.Map:
                    case SubstraitType.Struct:
                        return Cost.NestedWidth;
                    default:
                        return Cost.UnknownWidth;
                }
            }

            private static int Width(ReadRelation read, int column)
            {
                var types = read.BaseSchema.Struct?.Types;
                return types != null && column >= 0 && column < types.Count ? Width(types[column]) : Cost.UnknownWidth;
            }

            private Origin?[] ReadOrigins(ReadRelation read)
            {
                int length = read.OutputLength;
                var origins = new Origin?[length];
                if (_loopDepth > 0 || read.NamedTable.DotSeperated == "__gettimestamp")
                {
                    return origins;
                }
                List<int>? keys = null;
                if (!_underNormalization && _connectorManager != null)
                {
                    var names = _connectorManager.GetSourceFactory(read).GetRowStoreKeyColumns(read);
                    if (names != null)
                    {
                        keys = new List<int>();
                        foreach (var name in names)
                        {
                            int column = read.BaseSchema.Names.FindIndex(x => x.Equals(name, StringComparison.OrdinalIgnoreCase));
                            if (column < 0)
                            {
                                keys = null;
                                break;
                            }
                            keys.Add(column);
                        }
                    }
                }
                if (keys != null)
                {
                    // A source without keys keeps no rows of its own
                    Hosts.Add((read, read.NamedTable.DotSeperated, keys.Count == 0));
                }
                for (int i = 0; i < length; i++)
                {
                    int column = read.EmitSet ? read.Emit[i] : i;
                    if (keys != null && keys.Contains(column))
                    {
                        continue;
                    }
                    origins[i] = Make(read, i, keys != null, Width(read, column), column);
                }
                return origins;
            }

            private Origin?[] NormalizationOrigins(NormalizationRelation normalization)
            {
                bool before = _underNormalization;
                _underNormalization = true;
                Visit(normalization.Input, null!);
                _underNormalization = before;
                var widths = _origins[normalization.Input].Select(x => x?.Width ?? Cost.UnknownWidth).ToArray();
                Read(_origins[normalization.Input], normalization, normalization.Input);
                int length = normalization.OutputLength;
                var origins = new Origin?[length];
                if (_loopDepth > 0)
                {
                    return origins;
                }
                var read = normalization.Input as ReadRelation;
                Hosts.Add((normalization, read != null ? read.NamedTable.DotSeperated : "normalization", false));
                for (int i = 0; i < length; i++)
                {
                    int column = normalization.EmitSet ? normalization.Emit[i] : i;
                    if (normalization.KeyIndex.Contains(column))
                    {
                        continue;
                    }
                    int width = column >= 0 && column < widths.Length ? widths[column] : Cost.UnknownWidth;
                    origins[i] = Make(normalization, i, true, width, column);
                }
                return origins;
            }

            // The columns the expressions read, null for whole rows
            private bool[]? Needed(int length, IEnumerable<Expression?> expressions)
            {
                var need = new bool[length];
                var reader = new FieldReader(_functions, need);
                foreach (var expression in expressions)
                {
                    if (expression != null)
                    {
                        reader.Visit(expression, null);
                    }
                }
                return reader.Rows ? null : need;
            }

            private void Read(Origin?[] origins, bool[]? need, Relation reader, bool all, Relation input)
            {
                if (need == null || all)
                {
                    Read(origins, reader, input);
                }
                else
                {
                    Read(origins, need, reader, Share(input));
                }
            }

            // A group's reads are kept until its inputs are counted
            private Origin?[] FilterOrigins(FilterRelation filter, GroupReads? group)
            {
                Origin?[] origins;
                if (group != null && filter.Input is MergeJoinRelation below && below.MultiJoinGroup == group.Group)
                {
                    origins = GroupJoin(below, group);
                    group.Add(origins, Needed(origins.Length, [filter.Condition]));
                    return Emitted(filter, origins);
                }
                Visit(filter.Input, null!);
                origins = Of(filter.Input);
                Read(origins, Needed(origins.Length, [filter.Condition]), filter, false, filter.Input);
                return Emitted(filter, origins);
            }

            private Origin?[] ProjectOrigins(ProjectRelation project)
            {
                Visit(project.Input, null!);
                var input = Of(project.Input);
                var computed = new List<Expression>();
                foreach (var expression in project.Expressions)
                {
                    if (!IsPlain(expression, input.Length, out _))
                    {
                        computed.Add(expression);
                    }
                }
                Read(input, Needed(input.Length, computed), project, false, project.Input);
                var origins = new Origin?[input.Length + project.Expressions.Count];
                Array.Copy(input, origins, input.Length);
                for (int i = 0; i < project.Expressions.Count; i++)
                {
                    // A plain reference is a second name of its column
                    origins[input.Length + i] = IsPlain(project.Expressions[i], input.Length, out var field)
                        ? input[field]
                        : (_loopDepth == 0 ? Make(project, input.Length + i, false, Cost.UnknownWidth) : null);
                }
                return Emitted(project, origins);
            }

            // What the conditions of one marked group read
            private sealed class GroupReads
            {
                public required int Group { get; init; }
                public bool Rows { get; set; }
                public HashSet<Origin> Read { get; } = new HashSet<Origin>(ReferenceEqualityComparer.Instance);
                public List<(Origin?[] Origins, Relation Relation)> Inputs { get; } = new List<(Origin?[], Relation)>();

                public void Add(Origin?[] origins, bool[]? need)
                {
                    if (need == null)
                    {
                        Rows = true;
                        return;
                    }
                    for (int i = 0; i < origins.Length; i++)
                    {
                        if (need[i] && origins[i] != null)
                        {
                            Read.Add(origins[i]!);
                        }
                    }
                }
            }

            private Origin?[] JoinOrigins(MergeJoinRelation join)
            {
                if (join.MultiJoinGroup == null)
                {
                    Visit(join.Left, null!);
                    Visit(join.Right, null!);
                    var left = Of(join.Left);
                    var right = Of(join.Right);
                    var leftBefore = (Origin?[])left.Clone();
                    var rightBefore = (Origin?[])right.Clone();
                    var need = Needed(left.Length + right.Length, join.LeftKeys.Concat<Expression?>(join.RightKeys).Append(join.PostJoinFilter));
                    // A plain join under builder parallelism works on whole rows
                    bool all = _parallelism > 1;
                    // Each side has a fetch of its own
                    Read(left, need?.AsSpan(0, left.Length).ToArray(), SideFetch(join.Left), all, join.Left);
                    Read(right, need?.AsSpan(left.Length).ToArray(), SideFetch(join.Right), all, join.Right);
                    // Each input of a join keeps its rows
                    Stored(left, Share(join.Left), join.Left, leftBefore);
                    Stored(right, Share(join.Right), join.Right, rightBefore);
                    var both = left.Concat(right).ToArray();
                    return JoinOutput(join, both, left.Length);
                }
                var group = new GroupReads() { Group = join.MultiJoinGroup.Value };
                var origins = GroupJoin(join, group);
                var after = new Dictionary<Origin, Origin?>(ReferenceEqualityComparer.Instance);
                foreach (var (input, relation) in group.Inputs)
                {
                    double share = Share(relation);
                    var here = new HashSet<Origin>(ReferenceEqualityComparer.Instance);
                    for (int i = 0; i < input.Length; i++)
                    {
                        var origin = input[i];
                        if (origin == null || !here.Add(origin))
                        {
                            continue;
                        }
                        if (group.Rows || group.Read.Contains(origin))
                        {
                            // Read by a condition of the group, fetched under this input
                            var read = Read(origin, SideFetch(relation), i, share);
                            after.TryAdd(origin, read);
                        }
                        else
                        {
                            // A group keeps each input once, whatever its joins
                            origin.Sites.TryAdd(relation, share);
                            after.TryAdd(origin, origin);
                        }
                    }
                }
                for (int i = 0; i < origins.Length; i++)
                {
                    if (origins[i] != null && after.TryGetValue(origins[i]!, out var now))
                    {
                        origins[i] = now;
                    }
                }
                return origins;
            }

            private Origin?[] JoinOutput(MergeJoinRelation join, Origin?[] both, int leftLength)
            {
                if (join.Type == JoinType.LeftMark)
                {
                    var marked = new Origin?[leftLength + 1];
                    Array.Copy(both, marked, leftLength);
                    return Emitted(join, marked);
                }
                return Emitted(join, both);
            }

            // One join of a group, nothing is counted yet
            private Origin?[] GroupJoin(MergeJoinRelation join, GroupReads group)
            {
                var left = GroupSide(join.Left, group);
                var right = GroupSide(join.Right, group);
                var both = left.Concat(right).ToArray();
                group.Add(both, Needed(both.Length, join.LeftKeys.Concat<Expression?>(join.RightKeys).Append(join.PostJoinFilter)));
                return JoinOutput(join, both, left.Length);
            }

            private Origin?[] GroupSide(Relation input, GroupReads group)
            {
                if (input is MergeJoinRelation join && join.MultiJoinGroup == group.Group)
                {
                    var origins = GroupJoin(join, group);
                    _origins[input] = origins;
                    return (Origin?[])origins.Clone();
                }
                if (input is FilterRelation filter && filter.Input is MergeJoinRelation below && below.MultiJoinGroup == group.Group)
                {
                    var origins = FilterOrigins(filter, group);
                    _origins[input] = origins;
                    return (Origin?[])origins.Clone();
                }
                Visit(input, null!);
                var leaf = Of(input);
                group.Inputs.Add((leaf, input));
                return leaf;
            }

            private Origin?[] ExchangeOrigins(ExchangeRelation exchange)
            {
                Visit(exchange.Input, null!);
                var origins = Of(exchange.Input);
                bool passes = !exchange.EmitSet && exchange.ExchangeKind is ScatterExchangeKind or BroadcastExchangeKind;
                foreach (var target in exchange.Targets)
                {
                    passes &= target is StandardOutputExchangeTarget;
                }
                bool[]? need = new bool[origins.Length];
                if (exchange.ExchangeKind is ScatterExchangeKind scatter)
                {
                    need = Needed(origins.Length, scatter.Fields);
                }
                Read(origins, need, exchange, !passes, exchange.Input);
                return Emitted(exchange, origins);
            }

            private Origin?[] Target(int relationId, Relation reference)
            {
                if (relationId < 0 || relationId >= _relations.Count || _rootState[relationId] == 1 || _loopDepth > 0)
                {
                    return new Origin?[reference.OutputLength];
                }
                Root(relationId);
                var origins = Of(_relations[relationId]);
                if (origins.Length != reference.OutputLength)
                {
                    // The rewrite sizes its work from this width, it cannot be trusted
                    Mismatch = $"a reference to relation {relationId} says it has {reference.OutputLength} columns, the relation has {origins.Length}";
                    Read(origins, reference, _relations[relationId]);
                }
                return origins;
            }

            private Origin?[] UnionOrigins(SetRelation set)
            {
                bool same = true;
                foreach (var input in set.Inputs)
                {
                    Visit(input, null!);
                    same &= SameStored(_origins[input], _origins[set.Inputs[0]]);
                }
                if (!same)
                {
                    foreach (var input in set.Inputs)
                    {
                        Read(_origins[input], set, input);
                    }
                    return Emitted(set, Fresh(set, set.Inputs[0].OutputLength));
                }
                var origins = Of(set.Inputs[0]);
                for (int i = 0; i < origins.Length; i++)
                {
                    // Values of several inputs are new values here
                    if (origins[i] != null && !origins[i]!.Host)
                    {
                        origins[i] = Make(set, i, false, origins[i]!.Width);
                    }
                }
                return Emitted(set, origins);
            }

            // The inputs carry the same host columns at the same places
            private static bool SameStored(Origin?[] first, Origin?[] other)
            {
                if (first.Length != other.Length)
                {
                    return false;
                }
                for (int i = 0; i < first.Length; i++)
                {
                    bool hostFirst = first[i] != null && first[i]!.Host;
                    bool hostOther = other[i] != null && other[i]!.Host;
                    if (hostFirst != hostOther || (hostFirst && !ReferenceEquals(first[i], other[i])))
                    {
                        return false;
                    }
                }
                return true;
            }

            private Origin?[] WriteOrigins(WriteRelation write)
            {
                Visit(write.Input, null!);
                var origins = Of(write.Input);
                IReadOnlyList<int>? keys = null;
                if (_connectorManager != null && !write.EmitSet)
                {
                    keys = _connectorManager.GetSinkFactory(write).GetRowStoreFetchKeyColumns(write);
                }
                // A key outside the columns: the sink does not fetch
                if (keys != null && keys.Count > 0 && keys.All(x => x >= 0 && x < origins.Length))
                {
                    var need = new bool[origins.Length];
                    foreach (var key in keys)
                    {
                        need[key] = true;
                    }
                    var before = (Origin?[])origins.Clone();
                    // A stored key is fetched under the sink's own fetch
                    if (!_keyFetches.TryGetValue(write, out var keyFetch))
                    {
                        keyFetch = new ReferenceRelation();
                        _keyFetches.Add(write, keyFetch);
                        _standsFor.Add(keyFetch, write.Input);
                    }
                    Read(origins, need, keyFetch, Share(write.Input));
                    // The sink keeps its rows and reads the rest at upload
                    Stored(origins, Share(write.Input), write, before);
                }
                Read(origins, write, write.Input);
                return origins;
            }
        }

        /// <summary>
        /// Decides which hosts get a store, the plan is not changed.
        /// </summary>
        public static LateMaterializationReport Analyze(Plan plan, IConnectorManager? connectorManager, IFunctionsRegister functions, int parallelism)
        {
            var report = new LateMaterializationReport();
            if (RowStoreHostCollector.UsesRowStores(plan.Relations))
            {
                report.Skip("the plan has stores placed by hand");
                return report;
            }
            var scan = new ReferenceScan();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                scan.Visit(plan.Relations[i], null!);
            }
            if (scan.Shared)
            {
                report.Skip("a relation is used in two places");
                return report;
            }
            var analysis = new Analysis(plan.Relations, connectorManager, functions, parallelism, scan);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                analysis.Root(i);
            }
            foreach (var (host, table, addsCopy) in analysis.Hosts)
            {
                // A column read before any stored copy saves nothing
                var own = analysis.All.Where(x => ReferenceEquals(x.Producer, host) && x.Host).ToList();
                var columns = own.Where(x => x.Copies > 0).ToList();
                var readers = Readers(columns);
                // Columns that cost more than they save are dropped, never all
                bool dropped = true;
                while (dropped)
                {
                    dropped = false;
                    foreach (var reader in readers.Keys.ToList())
                    {
                        var without = columns.Where(x => !x.Readers.ContainsKey(reader)).ToList();
                        if (without.Count > 0 && without.Count < columns.Count && Gain(own, without, addsCopy) >= Gain(own, columns, addsCopy))
                        {
                            columns = without;
                            readers = Readers(without);
                            dropped = true;
                            break;
                        }
                    }
                }
                double saving = columns.Sum(x => x.Width * x.Copies);
                // The reference is kept where a column would have been
                double copies = Sites(columns).Values.Sum();
                double cost = saving - Gain(own, columns, addsCopy);
                var candidate = new RowStoreCandidate()
                {
                    Host = host,
                    Table = table,
                    Columns = columns.Select(x => x.Position).ToList(),
                    Copies = copies,
                    Fetches = readers.Count,
                    Saving = (long)Math.Round(saving),
                    Cost = (long)Math.Round(cost)
                };
                if (columns.Count == 0)
                {
                    candidate.Reason = "no column passes a stored copy before it is read";
                }
                else if (candidate.Net < Cost.Threshold)
                {
                    candidate.Reason = $"saves {candidate.Net} bytes per row, under {Cost.Threshold}";
                }
                else
                {
                    candidate.Admitted = true;
                }
                report.Hosts.Add(candidate);
            }
            if (analysis.Mismatch != null)
            {
                report.Skip(analysis.Mismatch);
            }
            foreach (var producer in analysis.All.Where(x => !x.Host && x.Copies > 0).GroupBy(x => x.Producer, (IEqualityComparer<Relation>)ReferenceEqualityComparer.Instance))
            {
                long saving = (long)Math.Round(producer.Sum(x => x.Width * x.Copies));
                if (saving >= Cost.Threshold)
                {
                    report.MidPlan.Add(new MidPlanCandidate() { Producer = producer.Key.GetType().Name, Columns = producer.Count(), Saving = saving });
                }
            }
            return report;
        }

        // What storing the columns saves per row, less all it costs
        private static double Gain(List<Origin> own, List<Origin> columns, bool addsCopy)
        {
            if (columns.Count == 0)
            {
                return 0;
            }
            // Without keys the heap is an added copy.
            // A keyed host keeps a reference and hash, and verifies.
            double identity = addsCopy ? Heap(columns) : Cost.Reference + Cost.Hash + Cost.Verification + KeptToo(own, columns);
            return columns.Sum(x => x.Width * x.Copies) - Cost.Reference * Sites(columns).Values.Sum() - identity - Cost.Fetch * Readers(columns).Values.Sum();
        }

        // Every place that keeps a copy of one of the columns
        private static Dictionary<Relation, double> Sites(List<Origin> columns)
        {
            var sites = new Dictionary<Relation, double>(ReferenceEqualityComparer.Instance);
            foreach (var (site, share) in columns.SelectMany(x => x.Sites))
            {
                sites[site] = sites.TryGetValue(site, out var before) ? Math.Max(before, share) : share;
            }
            return sites;
        }

        // Heap bytes per row, a column sent twice counts once
        private static double Heap(List<Origin> columns)
        {
            return columns.GroupBy(x => x.Column).Sum(x => (double)x.First().Width);
        }

        // A column sent twice and stored once stays in the host too:
        // the heap is an added copy of it
        private static double KeptToo(List<Origin> own, List<Origin> columns)
        {
            var kept = own.Where(x => !columns.Contains(x)).Select(x => x.Column).ToHashSet();
            return Heap(columns.Where(x => kept.Contains(x.Column)).ToList());
        }

        // Each place that reads a column back, by row share
        private static Dictionary<Relation, double> Readers(List<Origin> columns)
        {
            var readers = new Dictionary<Relation, double>(ReferenceEqualityComparer.Instance);
            foreach (var column in columns)
            {
                foreach (var (reader, share) in column.Readers)
                {
                    readers[reader] = readers.TryGetValue(reader, out var before) ? Math.Max(before, share) : share;
                }
            }
            return readers;
        }
    }
}
