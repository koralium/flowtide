﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    public sealed class MergeJoinRelation : Relation, IEquatable<MergeJoinRelation>
    {
        public JoinType Type { get; set; }

        public required Relation Left { get; set; }

        public required Relation Right { get; set; }

        public required List<FieldReference> LeftKeys { get; set; }

        public required List<FieldReference> RightKeys { get; set; }

        public Expression? PostJoinFilter { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit.Count;
                }
                return Left.OutputLength + Right.OutputLength;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitMergeJoinRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is MergeJoinRelation relation &&
                Equals(relation);
        }

        public bool Equals(MergeJoinRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Type, other.Type) &&
                Equals(Left, other.Left) &&
                Equals(Right, other.Right) &&
                LeftKeys.SequenceEqual(other.LeftKeys) &&
                RightKeys.SequenceEqual(other.RightKeys) &&
                Equals(PostJoinFilter, other.PostJoinFilter);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Type);
            code.Add(Left);
            code.Add(Right);

            foreach (var key in LeftKeys)
            {
                code.Add(key);
            }
            foreach (var key in RightKeys)
            {
                code.Add(key);
            }

            code.Add(PostJoinFilter);
            return code.ToHashCode();
        }

        public static bool operator ==(MergeJoinRelation? left, MergeJoinRelation? right)
        {
            return EqualityComparer<MergeJoinRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(MergeJoinRelation? left, MergeJoinRelation? right)
        {
            return !(left == right);
        }
    }
}
