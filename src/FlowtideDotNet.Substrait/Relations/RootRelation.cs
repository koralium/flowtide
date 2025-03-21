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


namespace FlowtideDotNet.Substrait.Relations
{
    public sealed class RootRelation : Relation, IEquatable<RootRelation>
    {
        public override int OutputLength => Input.OutputLength;

        public required Relation Input { get; set; }

        public required List<string> Names { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitRootRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is RootRelation relation &&
                Equals(relation);
        }

        public bool Equals(RootRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Names.SequenceEqual(other.Names);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            foreach (var name in Names)
            {
                code.Add(name);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(RootRelation? left, RootRelation? right)
        {
            return EqualityComparer<RootRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(RootRelation? left, RootRelation? right)
        {
            return !(left == right);
        }
    }
}
