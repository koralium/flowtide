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
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    /// <summary>
    /// Places row stores on a plan's hosts.
    /// A fetch goes before the first reader of a stored column.
    /// Relations between them pass references on in place of the columns.
    /// </summary>
    internal sealed class LateMaterializationPass : OptimizerBaseVisitor
    {
        // Where a column of the old plan is now
        private readonly struct Slot : IEquatable<Slot>
        {
            // The value's column, or the reference column when stored
            public readonly int Position;
            public readonly string? Store;
            public readonly int Stored;

            public Slot(int position)
            {
                Position = position;
                Store = null;
                Stored = -1;
            }

            public Slot(int position, string store, int stored)
            {
                Position = position;
                Store = store;
                Stored = stored;
            }

            public bool Equals(Slot other)
            {
                return Position == other.Position && Store == other.Store && Stored == other.Stored;
            }
        }

        private sealed class Layout
        {
            public required Slot[] Slots { get; init; }
            public required int Length { get; init; }
        }

        // Columns a join of the same group above needs as values
        private sealed class GroupDemand
        {
            public required int Group { get; init; }
            public required bool[] Need { get; init; }
            // A condition of the group works on whole rows
            public bool Rows { get; init; }
        }

        // Finds the columns an expression reads
        private sealed class FieldReader : ExpressionVisitor<object?, object?>
        {
            private readonly IFunctionsRegister _functions;
            private readonly bool[] _need;

            public FieldReader(IFunctionsRegister functions, bool[] need)
            {
                _functions = functions;
                _need = need;
            }

            // True when the input's columns must stay where they are
            public bool Rows { get; private set; }

            public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
            {
                if (directFieldReference.Root is RootReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment segment &&
                    segment.Field >= 0 && segment.Field < _need.Length)
                {
                    _need[segment.Field] = true;
                }
                else
                {
                    Rows = true;
                }
                return base.VisitDirectFieldReference(directFieldReference, state);
            }

            public override object? VisitScalarFunction(ScalarFunction scalarFunction, object? state)
            {
                if (!_functions.TryGetColumnScalarFunction(scalarFunction.ExtensionUrn, scalarFunction.ExtensionName, out _))
                {
                    Rows = true;
                }
                return base.VisitScalarFunction(scalarFunction, state);
            }

            public override object? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object? state)
            {
                Rows = true;
                return base.VisitSetPredicateExpression(setPredicateExpression, state);
            }
        }

        // Copies an expression with its fields at their new positions.
        // Plans share expression objects, so nothing is changed in place.
        private sealed class FieldMover : ExpressionVisitor<Expression, object?>
        {
            private readonly Dictionary<int, int> _positions;

            public FieldMover(Dictionary<int, int> positions)
            {
                _positions = positions;
            }

            public Expression Move(Expression expression)
            {
                return Visit(expression, null) ?? throw new InvalidOperationException($"Late materialization cannot copy a {expression.GetType().Name}.");
            }

            private List<Expression> Move(List<Expression> expressions)
            {
                var moved = new List<Expression>(expressions.Count);
                for (int i = 0; i < expressions.Count; i++)
                {
                    moved.Add(Move(expressions[i]));
                }
                return moved;
            }

            public override Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
            {
                if (directFieldReference.ReferenceSegment is not StructReferenceSegment segment || !_positions.TryGetValue(segment.Field, out var position))
                {
                    throw new InvalidOperationException("Late materialization left a column stored where an expression reads it.");
                }
                var moved = (StructReferenceSegment)segment.Clone();
                moved.Field = position;
                return new DirectFieldReference() { ReferenceSegment = moved, Root = directFieldReference.Root };
            }

            public override Expression? VisitStringLiteral(StringLiteral stringLiteral, object? state) => stringLiteral;

            public override Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, object? state)
            {
                return new ArrayLiteral() { Expressions = Move(arrayLiteral.Expressions) };
            }

            public override Expression? VisitNumericLiteral(NumericLiteral numericLiteral, object? state) => numericLiteral;

            public override Expression? VisitNullLiteral(NullLiteral nullLiteral, object? state) => nullLiteral;

            public override Expression? VisitBoolLiteral(BoolLiteral boolLiteral, object? state) => boolLiteral;

            public override Expression? VisitBinaryLiteral(BinaryLiteral binaryLiteral, object? state) => binaryLiteral;

            public override Expression? VisitScalarFunction(ScalarFunction scalarFunction, object? state)
            {
                return new ScalarFunction()
                {
                    ExtensionUrn = scalarFunction.ExtensionUrn,
                    ExtensionName = scalarFunction.ExtensionName,
                    Arguments = Move(scalarFunction.Arguments),
                    Options = scalarFunction.Options
                };
            }

            public override Expression? VisitIfThen(IfThenExpression ifThenExpression, object? state)
            {
                var ifs = new List<IfClause>(ifThenExpression.Ifs.Count);
                foreach (var clause in ifThenExpression.Ifs)
                {
                    ifs.Add(new IfClause() { If = Move(clause.If), Then = Move(clause.Then) });
                }
                return new IfThenExpression() { Ifs = ifs, Else = ifThenExpression.Else != null ? Move(ifThenExpression.Else) : null };
            }

            public override Expression? VisitSingularOrList(SingularOrListExpression singularOrList, object? state)
            {
                return new SingularOrListExpression() { Value = Move(singularOrList.Value), Options = Move(singularOrList.Options) };
            }

            public override Expression? VisitMultiOrList(MultiOrListExpression multiOrList, object? state)
            {
                var options = new List<OrListRecord>(multiOrList.Options.Count);
                foreach (var record in multiOrList.Options)
                {
                    options.Add(new OrListRecord() { Fields = Move(record.Fields) });
                }
                return new MultiOrListExpression() { Value = Move(multiOrList.Value), Options = options };
            }

            public override Expression? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, object? state)
            {
                var pairs = new List<KeyValuePair<Expression, Expression>>(mapNestedExpression.KeyValues.Count);
                foreach (var pair in mapNestedExpression.KeyValues)
                {
                    pairs.Add(new KeyValuePair<Expression, Expression>(Move(pair.Key), Move(pair.Value)));
                }
                return new MapNestedExpression() { KeyValues = pairs };
            }

            public override Expression? VisitListNestedExpression(ListNestedExpression listNestedExpression, object? state)
            {
                return new ListNestedExpression() { Values = Move(listNestedExpression.Values) };
            }

            public override Expression? VisitCastExpression(CastExpression castExpression, object? state)
            {
                return new CastExpression() { Expression = Move(castExpression.Expression), Type = castExpression.Type };
            }

            public override Expression? VisitStructExpression(StructExpression structExpression, object? state)
            {
                if (structExpression.GetType() != typeof(StructExpression))
                {
                    throw new InvalidOperationException($"Late materialization cannot copy a {structExpression.GetType().Name}.");
                }
                return new StructExpression() { Fields = Move(structExpression.Fields) };
            }

            public override Expression? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object? state)
            {
                throw new InvalidOperationException("Late materialization cannot move the columns under a subquery.");
            }
        }

        // Finds the numbered relations that must give plain values
        private sealed class ReferenceScan : BaseRelationExpressionVisitor<object?>
        {
            private sealed class SubqueryVisitor : ExpressionVisitor<object?, object>
            {
                private readonly ReferenceScan _scan;

                public SubqueryVisitor(ReferenceScan scan)
                {
                    _scan = scan;
                }

                public override object? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object state)
                {
                    _scan._closed++;
                    _scan.Visit(setPredicateExpression.Relation, state);
                    _scan._closed--;
                    return base.VisitSetPredicateExpression(setPredicateExpression, state);
                }
            }

            private readonly SubqueryVisitor _subqueryVisitor;
            // Inside a loop or a subquery, not rewritten
            private int _closed;

            public ReferenceScan()
            {
                _subqueryVisitor = new SubqueryVisitor(this);
            }

            public override ExpressionVisitor<object?, object> Visitor => _subqueryVisitor;

            public HashSet<int> Referenced { get; } = new HashSet<int>();

            public HashSet<int> Exact { get; } = new HashSet<int>();

            private readonly HashSet<Relation> _seen = new HashSet<Relation>(ReferenceEqualityComparer.Instance);

            // True when one relation object sits in two places
            public bool Shared { get; private set; }

            public override Relation Visit(Relation relation, object state)
            {
                if (!_seen.Add(relation))
                {
                    Shared = true;
                    return relation;
                }
                return base.Visit(relation, state);
            }

            private void Add(Relation reference, int relationId)
            {
                Referenced.Add(relationId);
                if (_closed > 0 || reference.EmitSet)
                {
                    Exact.Add(relationId);
                }
            }

            public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
            {
                _closed++;
                var result = base.VisitIterationRelation(iterationRelation, state);
                _closed--;
                return result;
            }

            public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
            {
                Add(referenceRelation, referenceRelation.RelationId);
                return base.VisitReferenceRelation(referenceRelation, state);
            }

            public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
            {
                Add(standardOutputExchangeReferenceRelation, standardOutputExchangeReferenceRelation.RelationId);
                return base.VisitStandardOutputExchangeReferenceRelation(standardOutputExchangeReferenceRelation, state);
            }
        }

        // Columns the reader above a union needs as values
        private sealed class ColumnDemand
        {
            public required bool[] Need { get; init; }
        }

        // State of a visit whose caller takes references
        private static readonly object Carry = new object();

        // A union fetches in its inputs what its reader needs,
        // a project hands the need on to one below it
        private static object Ask(Relation input, bool[]? need)
        {
            if (input is ProjectRelation || (input is SetRelation set && set.Operation == SetOperation.UnionAll))
            {
                return need != null ? new ColumnDemand() { Need = need } : null!;
            }
            return Carry;
        }

        private readonly List<Relation> _relations;
        private readonly IConnectorManager? _connectorManager;
        private readonly IFunctionsRegister _functions;
        private readonly int _parallelism;
        private readonly Dictionary<Relation, Layout> _layouts = new Dictionary<Relation, Layout>(ReferenceEqualityComparer.Instance);
        // 0 not visited, 1 being visited, 2 done
        private readonly int[] _rootState;
        private readonly HashSet<int> _referenced;
        private readonly HashSet<int> _exactRoots;
        private int _loopDepth;
        private bool _underNormalization;
        private int _stores;

        private LateMaterializationPass(List<Relation> relations, IConnectorManager? connectorManager, IFunctionsRegister functions, int parallelism, ReferenceScan scan)
        {
            _relations = relations;
            _connectorManager = connectorManager;
            _functions = functions;
            _parallelism = parallelism;
            _rootState = new int[relations.Count];
            _referenced = scan.Referenced;
            _exactRoots = scan.Exact;
        }

        // Rewrites the plan in place, returns the number of stores placed
        public static int Run(Plan plan, IConnectorManager? connectorManager, IFunctionsRegister functions, int parallelism)
        {
            if (RowStoreHostCollector.UsesRowStores(plan.Relations))
            {
                // Stores placed by hand are left as they are
                return 0;
            }
            var scan = new ReferenceScan();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                scan.Visit(plan.Relations[i], null!);
            }
            if (scan.Shared)
            {
                // Rewritten in place, a shared relation would be rewritten twice
                return 0;
            }
            var pass = new LateMaterializationPass(plan.Relations, connectorManager, functions, parallelism, scan);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                pass.Root(i);
            }
            return pass._stores;
        }

        private void Root(int index)
        {
            if (_rootState[index] != 0)
            {
                return;
            }
            _rootState[index] = 1;
            // A numbered relation is outside the loop reading it
            int loopDepth = _loopDepth;
            bool underNormalization = _underNormalization;
            _loopDepth = 0;
            _underNormalization = false;
            // Only a relation read through a reference can give references
            var relation = Visit(_relations[index], _referenced.Contains(index) && !_exactRoots.Contains(index) ? Carry : null!);
            if (_exactRoots.Contains(index))
            {
                if (relation is ExchangeRelation && Of(relation) != null)
                {
                    throw new InvalidOperationException($"Late materialization found exchange {index} reading its own output.");
                }
                // Found to read its own output while it was visited
                relation = Fetch(relation, null);
            }
            _loopDepth = loopDepth;
            _underNormalization = underNormalization;
            _relations[index] = relation;
            _rootState[index] = 2;
        }

        private static bool PassesReferences(Relation relation)
        {
            switch (relation)
            {
                case ReadRelation:
                case NormalizationRelation:
                case FilterRelation:
                case ProjectRelation:
                case MergeJoinRelation:
                case ExchangeRelation:
                case ReferenceRelation:
                case StandardOutputExchangeReferenceRelation:
                    return true;
                case SetRelation set:
                    return set.Operation == SetOperation.UnionAll;
                default:
                    return false;
            }
        }

        // A null state asks for plain values, as before
        public override Relation Visit(Relation relation, object state)
        {
            if (_loopDepth > 0)
            {
                state = null!;
            }
            // Any other relation reads whole rows, its inputs give values
            var result = relation.Accept(this, PassesReferences(relation) ? state : null!);
            if (state == null)
            {
                return Fetch(result, null);
            }
            if (state is GroupDemand demand)
            {
                return Fetch(result, demand.Need);
            }
            if (state is ColumnDemand columns)
            {
                return Fetch(result, columns.Need);
            }
            return result;
        }

        public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
        {
            _loopDepth++;
            var result = base.VisitIterationRelation(iterationRelation, null!);
            _loopDepth--;
            return result;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (_loopDepth > 0 || _underNormalization || _connectorManager == null || readRelation.NamedTable.DotSeperated == "__gettimestamp")
            {
                return readRelation;
            }
            var keyNames = _connectorManager.GetSourceFactory(readRelation).GetRowStoreKeyColumns(readRelation);
            if (keyNames == null)
            {
                return readRelation;
            }
            var keys = new List<int>();
            for (int i = 0; i < keyNames.Count; i++)
            {
                var keyName = keyNames[i];
                int column = readRelation.BaseSchema.Names.FindIndex(x => x.Equals(keyName, StringComparison.OrdinalIgnoreCase));
                if (column < 0)
                {
                    // A key outside the schema, its columns are not known
                    return readRelation;
                }
                keys.Add(column);
            }
            readRelation.RowStore = Host(readRelation, readRelation.BaseSchema.Names.Count, keys, readRelation.NamedTable.DotSeperated);
            return readRelation;
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            bool before = _underNormalization;
            _underNormalization = true;
            normalizationRelation.Input = Visit(normalizationRelation.Input, null!);
            _underNormalization = before;
            if (_loopDepth == 0)
            {
                var name = normalizationRelation.Input is ReadRelation read ? read.NamedTable.DotSeperated : "normalization";
                normalizationRelation.RowStore = Host(normalizationRelation, normalizationRelation.Input.OutputLength, normalizationRelation.KeyIndex, name);
            }
            return normalizationRelation;
        }

        // Stores every output column that is not a key
        private RowStoreDefinition? Host(Relation relation, int columnCount, List<int> keys, string table)
        {
            int length = relation.EmitSet ? relation.Emit.Count : columnCount;
            var stored = new List<int>();
            for (int i = 0; i < length; i++)
            {
                if (!keys.Contains(relation.EmitSet ? relation.Emit[i] : i))
                {
                    stored.Add(i);
                }
            }
            if (stored.Count == 0)
            {
                return null;
            }
            var name = $"lm{_stores}_{table}";
            _stores++;
            var slots = new Slot[length];
            int kept = 0;
            for (int i = 0; i < length; i++)
            {
                int index = stored.IndexOf(i);
                slots[i] = index < 0 ? new Slot(kept++) : new Slot(length - stored.Count, name, index);
            }
            Register(relation, slots, length - stored.Count + 1);
            return new RowStoreDefinition() { Name = name, StoredColumns = stored };
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
        {
            var layout = Target(referenceRelation.RelationId);
            if (layout != null)
            {
                referenceRelation.ReferenceOutputLength = layout.Length;
                Register(referenceRelation, layout.Slots, layout.Length);
            }
            return referenceRelation;
        }

        public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
        {
            var layout = Target(standardOutputExchangeReferenceRelation.RelationId);
            if (layout != null)
            {
                standardOutputExchangeReferenceRelation.ReferenceOutputLength = layout.Length;
                Register(standardOutputExchangeReferenceRelation, layout.Slots, layout.Length);
            }
            return standardOutputExchangeReferenceRelation;
        }

        // A numbered relation's layout, null for plain values
        private Layout? Target(int relationId)
        {
            if (relationId < 0 || relationId >= _relations.Count)
            {
                return null;
            }
            if (_rootState[relationId] == 1)
            {
                // A relation that reads its own output gives values
                _exactRoots.Add(relationId);
                return null;
            }
            Root(relationId);
            return Of(_relations[relationId]);
        }

        public override Relation VisitExchangeRelation(ExchangeRelation exchangeRelation, object state)
        {
            int length = exchangeRelation.Input.OutputLength;
            var routedOn = new bool[length];
            if (exchangeRelation.ExchangeKind is ScatterExchangeKind asked)
            {
                var early = new FieldReader(_functions, routedOn);
                for (int i = 0; i < asked.Fields.Count; i++)
                {
                    early.Visit(asked.Fields[i], null);
                }
            }
            var input = Visit(exchangeRelation.Input, Ask(exchangeRelation.Input, routedOn));
            if (Of(input) != null)
            {
                // Asked for plain values, the exchange's input gives them:
                // its readers need the exchange itself, not a fetch on it
                bool passes = state != null && !exchangeRelation.EmitSet && exchangeRelation.ExchangeKind is ScatterExchangeKind or BroadcastExchangeKind;
                for (int i = 0; i < exchangeRelation.Targets.Count; i++)
                {
                    passes &= exchangeRelation.Targets[i] is StandardOutputExchangeTarget;
                }
                bool[]? need = null;
                if (passes)
                {
                    need = new bool[length];
                    if (exchangeRelation.ExchangeKind is ScatterExchangeKind scatter)
                    {
                        var reader = new FieldReader(_functions, need);
                        for (int i = 0; i < scatter.Fields.Count; i++)
                        {
                            reader.Visit(scatter.Fields[i], null);
                        }
                        if (reader.Rows)
                        {
                            need = null;
                        }
                    }
                }
                input = Fetch(input, need);
            }
            exchangeRelation.Input = input;
            var layout = Of(input);
            if (layout == null)
            {
                return exchangeRelation;
            }
            if (exchangeRelation.ExchangeKind is ScatterExchangeKind routed)
            {
                var mover = new FieldMover(Values(layout.Slots));
                var fields = new List<FieldReference>(routed.Fields.Count);
                for (int i = 0; i < routed.Fields.Count; i++)
                {
                    fields.Add((FieldReference)mover.Move(routed.Fields[i]));
                }
                exchangeRelation.ExchangeKind = new ScatterExchangeKind() { Fields = fields };
            }
            Register(exchangeRelation, layout.Slots, layout.Length);
            return exchangeRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            var need = new bool[filterRelation.Input.OutputLength];
            var reader = new FieldReader(_functions, need);
            reader.Visit(filterRelation.Condition, null);
            object inputState = Ask(filterRelation.Input, reader.Rows ? null : need);
            if (state is GroupDemand demand && filterRelation.Input is MergeJoinRelation join && join.MultiJoinGroup == demand.Group)
            {
                // Nothing sits between a group's joins, the need goes down
                for (int i = 0; i < demand.Need.Length; i++)
                {
                    if (demand.Need[i])
                    {
                        need[filterRelation.EmitSet ? filterRelation.Emit[i] : i] = true;
                    }
                }
                if (reader.Rows || demand.Rows)
                {
                    Array.Fill(need, true);
                }
                inputState = new GroupDemand() { Group = demand.Group, Need = need, Rows = demand.Rows };
            }
            var input = Fetch(Visit(filterRelation.Input, inputState), reader.Rows ? null : need);
            filterRelation.Input = input;
            var layout = Of(input);
            if (layout != null)
            {
                filterRelation.Condition = new FieldMover(Values(layout.Slots)).Move(filterRelation.Condition);
                ApplyEmit(filterRelation, layout.Slots, layout.Length);
            }
            return filterRelation;
        }

        // A plain field reference, it can pass a reference on
        private static bool IsPlain(Expression expression, int inputLength, out int field)
        {
            if (expression is DirectFieldReference direct &&
                direct.Root is RootReference &&
                direct.ReferenceSegment is StructReferenceSegment segment &&
                segment.Child == null &&
                segment.Field >= 0 && segment.Field < inputLength)
            {
                field = segment.Field;
                return true;
            }
            field = -1;
            return false;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            int inputLength = projectRelation.Input.OutputLength;
            var need = new bool[inputLength];
            var reader = new FieldReader(_functions, need);
            for (int i = 0; i < projectRelation.Expressions.Count; i++)
            {
                if (!IsPlain(projectRelation.Expressions[i], inputLength, out _))
                {
                    reader.Visit(projectRelation.Expressions[i], null);
                }
            }
            // A project drops no row: what its reader needs can be fetched
            // under it, where a union below does so in each partition
            bool passesDown = projectRelation.Input is ProjectRelation || (projectRelation.Input is SetRelation below && below.Operation == SetOperation.UnionAll);
            var down = need;
            if (passesDown && (state == null || state is ColumnDemand))
            {
                down = (bool[])need.Clone();
                int outputs = projectRelation.EmitSet ? projectRelation.Emit.Count : inputLength + projectRelation.Expressions.Count;
                for (int i = 0; i < outputs; i++)
                {
                    if (state is ColumnDemand asked && !asked.Need[i])
                    {
                        continue;
                    }
                    int column = projectRelation.EmitSet ? projectRelation.Emit[i] : i;
                    if (column < inputLength)
                    {
                        down[column] = true;
                    }
                    else if (IsPlain(projectRelation.Expressions[column - inputLength], inputLength, out var named))
                    {
                        down[named] = true;
                    }
                }
            }
            bool all = reader.Rows;
            var input = Fetch(Visit(projectRelation.Input, Ask(projectRelation.Input, all ? null : down)), all ? null : down);
            projectRelation.Input = input;
            var layout = Of(input);
            if (layout == null)
            {
                return projectRelation;
            }
            var replace = new FieldMover(Values(layout.Slots));
            var slots = new List<Slot>(layout.Slots);
            var kept = new List<Expression>();
            for (int i = 0; i < projectRelation.Expressions.Count; i++)
            {
                var expression = projectRelation.Expressions[i];
                if (IsPlain(expression, inputLength, out var field) && layout.Slots[field].Store != null)
                {
                    // The same stored column under a second position
                    slots.Add(layout.Slots[field]);
                    continue;
                }
                slots.Add(new Slot(layout.Length + kept.Count));
                kept.Add(replace.Move(expression));
            }
            projectRelation.Expressions = kept;
            ApplyEmit(projectRelation, slots.ToArray(), layout.Length + kept.Count);
            return projectRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            int leftLength = mergeJoinRelation.Left.OutputLength;
            int rightLength = mergeJoinRelation.Right.OutputLength;
            var need = new bool[leftLength + rightLength];
            var reader = new FieldReader(_functions, need);
            for (int i = 0; i < mergeJoinRelation.LeftKeys.Count; i++)
            {
                reader.Visit(mergeJoinRelation.LeftKeys[i], null);
            }
            for (int i = 0; i < mergeJoinRelation.RightKeys.Count; i++)
            {
                reader.Visit(mergeJoinRelation.RightKeys[i], null);
            }
            if (mergeJoinRelation.PostJoinFilter != null)
            {
                reader.Visit(mergeJoinRelation.PostJoinFilter, null);
            }
            bool mark = mergeJoinRelation.Type == JoinType.LeftMark;
            bool rows = reader.Rows;
            if (state is GroupDemand demand && demand.Group == mergeJoinRelation.MultiJoinGroup)
            {
                rows |= demand.Rows;
                for (int i = 0; i < demand.Need.Length; i++)
                {
                    int column = mergeJoinRelation.EmitSet ? mergeJoinRelation.Emit[i] : i;
                    if (demand.Need[i] && (!mark || column < leftLength))
                    {
                        need[column] = true;
                    }
                }
            }
            else if (mergeJoinRelation.MultiJoinGroup != null)
            {
                // A multi join may move a condition to any input
                rows |= GroupReadsRows(mergeJoinRelation, mergeJoinRelation.MultiJoinGroup.Value);
            }
            // A plain join under builder parallelism partitions rows as row events
            if (rows || (mergeJoinRelation.MultiJoinGroup == null && _parallelism > 1))
            {
                Array.Fill(need, true);
            }
            mergeJoinRelation.Left = Side(mergeJoinRelation.Left, mergeJoinRelation.MultiJoinGroup, need.AsSpan(0, leftLength).ToArray(), rows);
            mergeJoinRelation.Right = Side(mergeJoinRelation.Right, mergeJoinRelation.MultiJoinGroup, need.AsSpan(leftLength, rightLength).ToArray(), rows);
            var left = Of(mergeJoinRelation.Left);
            var right = Of(mergeJoinRelation.Right);
            int leftNow = left != null ? left.Length : leftLength;
            int rightNow = right != null ? right.Length : rightLength;
            var both = new Slot[leftLength + rightLength];
            for (int i = 0; i < leftLength; i++)
            {
                both[i] = left != null ? left.Slots[i] : new Slot(i);
            }
            for (int i = 0; i < rightLength; i++)
            {
                var slot = right != null ? right.Slots[i] : new Slot(i);
                both[leftLength + i] = slot.Store == null ? new Slot(leftNow + slot.Position) : new Slot(leftNow + slot.Position, slot.Store, slot.Stored);
            }
            if (left == null && right == null)
            {
                return mergeJoinRelation;
            }
            // Partitions of a join share key lists, each gets its own
            var replace = new FieldMover(Values(both));
            var leftKeys = new List<FieldReference>(mergeJoinRelation.LeftKeys.Count);
            for (int i = 0; i < mergeJoinRelation.LeftKeys.Count; i++)
            {
                leftKeys.Add((FieldReference)replace.Move(mergeJoinRelation.LeftKeys[i]));
            }
            var rightKeys = new List<FieldReference>(mergeJoinRelation.RightKeys.Count);
            for (int i = 0; i < mergeJoinRelation.RightKeys.Count; i++)
            {
                rightKeys.Add((FieldReference)replace.Move(mergeJoinRelation.RightKeys[i]));
            }
            mergeJoinRelation.LeftKeys = leftKeys;
            mergeJoinRelation.RightKeys = rightKeys;
            if (mergeJoinRelation.PostJoinFilter != null)
            {
                mergeJoinRelation.PostJoinFilter = replace.Move(mergeJoinRelation.PostJoinFilter);
            }
            if (mark)
            {
                // The mark is a value after the left columns
                var marked = new Slot[leftLength + 1];
                Array.Copy(both, marked, leftLength);
                marked[leftLength] = new Slot(leftNow);
                ApplyEmit(mergeJoinRelation, marked, leftNow + 1);
            }
            else
            {
                ApplyEmit(mergeJoinRelation, both, leftNow + rightNow);
            }
            return mergeJoinRelation;
        }

        // One input of a join with the needed columns as values
        private bool GroupReadsRows(Relation relation, int group)
        {
            if (relation is FilterRelation filter && filter.Input is MergeJoinRelation below && below.MultiJoinGroup == group)
            {
                var reader = new FieldReader(_functions, new bool[filter.Input.OutputLength]);
                reader.Visit(filter.Condition, null);
                return reader.Rows || GroupReadsRows(filter.Input, group);
            }
            if (relation is MergeJoinRelation join && join.MultiJoinGroup == group)
            {
                var reader = new FieldReader(_functions, new bool[join.Left.OutputLength + join.Right.OutputLength]);
                for (int i = 0; i < join.LeftKeys.Count; i++)
                {
                    reader.Visit(join.LeftKeys[i], null);
                }
                for (int i = 0; i < join.RightKeys.Count; i++)
                {
                    reader.Visit(join.RightKeys[i], null);
                }
                if (join.PostJoinFilter != null)
                {
                    reader.Visit(join.PostJoinFilter, null);
                }
                return reader.Rows || GroupReadsRows(join.Left, group) || GroupReadsRows(join.Right, group);
            }
            return false;
        }

        private Relation Side(Relation input, int? group, bool[] need, bool rows)
        {
            bool sameGroup = group != null &&
                ((input is MergeJoinRelation join && join.MultiJoinGroup == group) ||
                (input is FilterRelation filter && filter.Input is MergeJoinRelation below && below.MultiJoinGroup == group));
            if (sameGroup)
            {
                return Visit(input, new GroupDemand() { Group = group!.Value, Need = need, Rows = rows });
            }
            return Fetch(Visit(input, Ask(input, need)), need);
        }

        public override Relation VisitSetRelation(SetRelation setRelation, object state)
        {
            if (setRelation.Operation != SetOperation.UnionAll)
            {
                // Any other set operation compares whole rows
                return base.VisitSetRelation(setRelation, null!);
            }
            bool[]? need = null;
            if (state is ColumnDemand demand)
            {
                need = new bool[setRelation.Inputs[0].OutputLength];
                for (int i = 0; i < demand.Need.Length; i++)
                {
                    if (demand.Need[i])
                    {
                        need[setRelation.EmitSet ? setRelation.Emit[i] : i] = true;
                    }
                }
            }
            bool same = true;
            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                // What the reader needs is fetched before the merge
                // A union inside this one fetches in its own inputs
                var input = setRelation.Inputs[i];
                setRelation.Inputs[i] = Visit(input, need != null ? Ask(input, need) : state == null ? Ask(input, null) : Carry);
                if (need != null)
                {
                    setRelation.Inputs[i] = Fetch(setRelation.Inputs[i], need);
                }
                same &= Same(Of(setRelation.Inputs[0]), Of(setRelation.Inputs[i]));
            }
            // Values wanted above: each input fetches, partitions stay parallel
            if (!same || state == null)
            {
                // One fetch above could not read every input's column
                for (int i = 0; i < setRelation.Inputs.Count; i++)
                {
                    setRelation.Inputs[i] = Fetch(setRelation.Inputs[i], null);
                }
                return setRelation;
            }
            var layout = Of(setRelation.Inputs[0]);
            if (layout != null)
            {
                ApplyEmit(setRelation, layout.Slots, layout.Length);
            }
            return setRelation;
        }

        private static bool Same(Layout? first, Layout? other)
        {
            if (first == null || other == null)
            {
                return first == other;
            }
            return first.Length == other.Length && first.Slots.AsSpan().SequenceEqual(other.Slots);
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            int length = writeRelation.Input.OutputLength;
            IReadOnlyList<int>? keys = null;
            if (_connectorManager != null && !writeRelation.EmitSet)
            {
                keys = _connectorManager.GetSinkFactory(writeRelation).GetRowStoreFetchKeyColumns(writeRelation);
                if (keys != null && keys.Count == 0)
                {
                    keys = null;
                }
            }
            // The sink reads the rest itself, its keys come as values
            var need = new bool[length];
            for (int i = 0; keys != null && i < keys.Count; i++)
            {
                if (keys[i] >= 0 && keys[i] < length)
                {
                    need[keys[i]] = true;
                }
            }
            var input = Visit(writeRelation.Input, keys != null ? Ask(writeRelation.Input, need) : null!);
            if (keys != null)
            {
                input = Fetch(input, need);
            }
            writeRelation.Input = Fetch(input, null);
            return writeRelation;
        }

        private Layout? Of(Relation relation)
        {
            return _layouts.TryGetValue(relation, out var layout) ? layout : null;
        }

        // Old to new positions of the columns that hold values
        private static Dictionary<int, int> Values(Slot[] slots)
        {
            var values = new Dictionary<int, int>();
            for (int i = 0; i < slots.Length; i++)
            {
                if (slots[i].Store == null)
                {
                    values.Add(i, slots[i].Position);
                }
            }
            return values;
        }

        private void Register(Relation relation, Slot[] slots, int length)
        {
            bool plain = length == slots.Length;
            for (int i = 0; i < slots.Length && plain; i++)
            {
                plain = slots[i].Store == null && slots[i].Position == i;
            }
            if (plain)
            {
                _layouts.Remove(relation);
            }
            else
            {
                _layouts[relation] = new Layout() { Slots = slots, Length = length };
            }
        }

        // Rewrites a relation's emit, slots are per column before the emit
        private void ApplyEmit(Relation relation, Slot[] slots, int length)
        {
            if (!relation.EmitSet)
            {
                Register(relation, slots, length);
                return;
            }
            var emit = new List<int>();
            var emitted = new Slot[relation.Emit.Count];
            var references = new Dictionary<int, int>();
            for (int i = 0; i < emitted.Length; i++)
            {
                var slot = slots[relation.Emit[i]];
                if (slot.Store == null)
                {
                    emitted[i] = new Slot(emit.Count);
                    emit.Add(slot.Position);
                    continue;
                }
                if (!references.TryGetValue(slot.Position, out var reference))
                {
                    // One reference column serves every stored column emitted
                    reference = emit.Count;
                    references.Add(slot.Position, reference);
                    emit.Add(slot.Position);
                }
                emitted[i] = new Slot(reference, slot.Store, slot.Stored);
            }
            relation.Emit = emit;
            Register(relation, emitted, emit.Count);
        }

        // Reads stored columns back, all when need is null
        private Relation Fetch(Relation relation, bool[]? need)
        {
            var layout = Of(relation);
            if (layout == null)
            {
                return relation;
            }
            var slots = (Slot[])layout.Slots.Clone();
            var fetches = new List<RowStoreFetch>();
            bool storedLeft = false;
            for (int i = 0; i < slots.Length; i++)
            {
                if (slots[i].Store == null)
                {
                    continue;
                }
                if (need != null && !need[i])
                {
                    storedLeft = true;
                    continue;
                }
                var fetch = fetches.Find(x => x.ReferenceColumn == slots[i].Position);
                if (fetch == null)
                {
                    fetch = new RowStoreFetch() { ReferenceColumn = slots[i].Position, StoreName = slots[i].Store!, Columns = new List<int>() };
                    fetches.Add(fetch);
                }
                if (!fetch.Columns.Contains(slots[i].Stored))
                {
                    fetch.Columns.Add(slots[i].Stored);
                }
            }
            if (fetches.Count == 0)
            {
                if (need == null)
                {
                    throw new InvalidOperationException("Late materialization moved columns without storing one, the plan cannot be put back.");
                }
                return relation;
            }
            int length = layout.Length;
            var fetchStart = new int[fetches.Count];
            for (int i = 0; i < fetches.Count; i++)
            {
                fetchStart[i] = length;
                length += fetches[i].Columns.Count;
            }
            var result = new FetchRequiredColumnsRelation() { Input = relation, Fetches = fetches };
            // A reference column nothing reads through is dropped
            var keep = new bool[layout.Length];
            Array.Fill(keep, true);
            for (int i = 0; i < fetches.Count; i++)
            {
                keep[fetches[i].ReferenceColumn] = false;
            }
            for (int i = 0; i < slots.Length; i++)
            {
                if (slots[i].Store != null && need != null && !need[i])
                {
                    keep[slots[i].Position] = true;
                }
            }
            var emit = new List<int>();
            var moved = new int[layout.Length];
            for (int i = 0; i < layout.Length && storedLeft; i++)
            {
                moved[i] = emit.Count;
                if (keep[i])
                {
                    emit.Add(i);
                }
            }
            for (int i = 0; i < slots.Length; i++)
            {
                bool fetched = slots[i].Store != null && (need == null || need[i]);
                int position = slots[i].Position;
                if (fetched)
                {
                    int index = fetches.FindIndex(x => x.ReferenceColumn == slots[i].Position);
                    position = fetchStart[index] + fetches[index].Columns.IndexOf(slots[i].Stored);
                }
                if (!storedLeft)
                {
                    // Every column is a value again, in the order it had
                    emit.Add(position);
                    continue;
                }
                if (fetched)
                {
                    // Two names of one stored column each get a column
                    slots[i] = new Slot(emit.Count);
                    emit.Add(position);
                }
                else
                {
                    slots[i] = slots[i].Store == null ? new Slot(moved[position]) : new Slot(moved[position], slots[i].Store!, slots[i].Stored);
                }
            }
            bool asIs = emit.Count == length;
            for (int i = 0; i < emit.Count && asIs; i++)
            {
                asIs = emit[i] == i;
            }
            if (!asIs)
            {
                result.Emit = emit;
            }
            if (storedLeft)
            {
                Register(result, slots, emit.Count);
            }
            return result;
        }
    }
}
