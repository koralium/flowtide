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

using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Substrait.Relations
{
    /// <summary>
    /// Passes its input through and tracks the issues raised by its checks.
    /// </summary>
    public sealed class CheckRelation : Relation, IEquatable<CheckRelation>
    {
        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit!.Count;
                }
                return Input.OutputLength;
            }
        }

        public required Relation Input { get; set; }

        /// <summary>
        /// Checks evaluated against each input row.
        /// </summary>
        public required List<CheckDefinition> Checks { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitCheckRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is CheckRelation relation &&
                Equals(relation);
        }

        public bool Equals(CheckRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Checks.SequenceEqual(other.Checks);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            foreach (var check in Checks)
            {
                code.Add(check);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(CheckRelation? left, CheckRelation? right)
        {
            return EqualityComparer<CheckRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(CheckRelation? left, CheckRelation? right)
        {
            return !(left == right);
        }
    }

    /// <summary>
    /// One check, all expressions are evaluated against the input row.
    /// </summary>
    public sealed class CheckDefinition : IEquatable<CheckDefinition>
    {
        /// <summary>
        /// Raises an issue when it evaluates to Boolean false.
        /// </summary>
        public required Expression Condition { get; set; }

        /// <summary>
        /// Check name template, placeholders like {tag} are left unrendered.
        /// </summary>
        public required string Message { get; set; }

        public required List<CheckTag> Tags { get; set; }

        /// <summary>
        /// Must all hold before the check is evaluated, empty means always.
        /// </summary>
        public required List<CheckGuard> Guards { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is CheckDefinition definition &&
                Equals(definition);
        }

        public bool Equals(CheckDefinition? other)
        {
            return other != null &&
                Equals(Condition, other.Condition) &&
                string.Equals(Message, other.Message, StringComparison.Ordinal) &&
                Tags.SequenceEqual(other.Tags) &&
                Guards.SequenceEqual(other.Guards);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Condition);
            code.Add(Message, StringComparer.Ordinal);
            foreach (var tag in Tags)
            {
                code.Add(tag);
            }
            foreach (var guard in Guards)
            {
                code.Add(guard);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(CheckDefinition? left, CheckDefinition? right)
        {
            return EqualityComparer<CheckDefinition>.Default.Equals(left, right);
        }

        public static bool operator !=(CheckDefinition? left, CheckDefinition? right)
        {
            return !(left == right);
        }
    }

    /// <summary>
    /// Named tag value attached to an issue.
    /// </summary>
    public sealed class CheckTag : IEquatable<CheckTag>
    {
        public required string Key { get; set; }

        public required Expression Value { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is CheckTag tag &&
                Equals(tag);
        }

        public bool Equals(CheckTag? other)
        {
            return other != null &&
                Equals(Key, other.Key) &&
                Equals(Value, other.Value);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Key, Value);
        }

        public static bool operator ==(CheckTag? left, CheckTag? right)
        {
            return EqualityComparer<CheckTag>.Default.Equals(left, right);
        }

        public static bool operator !=(CheckTag? left, CheckTag? right)
        {
            return !(left == right);
        }
    }

    /// <summary>
    /// Condition that must hold before a check is evaluated.
    /// </summary>
    public sealed class CheckGuard : IEquatable<CheckGuard>
    {
        public required Expression Expression { get; set; }

        public required CheckGuardKind Kind { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is CheckGuard guard &&
                Equals(guard);
        }

        public bool Equals(CheckGuard? other)
        {
            return other != null &&
                Equals(Expression, other.Expression) &&
                Kind == other.Kind;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Expression, Kind);
        }

        public static bool operator ==(CheckGuard? left, CheckGuard? right)
        {
            return EqualityComparer<CheckGuard>.Default.Equals(left, right);
        }

        public static bool operator !=(CheckGuard? left, CheckGuard? right)
        {
            return !(left == right);
        }
    }

    public enum CheckGuardKind
    {
        /// <summary>
        /// The expression is true, the CASE branch was taken.
        /// </summary>
        IsTrue = 0,

        /// <summary>
        /// The expression is not true, an earlier CASE branch was not taken.
        /// </summary>
        IsNotTrue = 1,

        /// <summary>
        /// The expression is null, an earlier COALESCE argument was null.
        /// </summary>
        IsNull = 2
    }
}
