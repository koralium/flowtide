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

using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Relations
{
    public class WriteRelation : Relation, IEquatable<WriteRelation>
    {
        public override int OutputLength => Input.OutputLength;

        public required Relation Input { get; set; }

        public required NamedStruct TableSchema { get; set; }

        public required NamedTable NamedObject { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitWriteRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is WriteRelation relation &&
                Equals(relation);
        }

        public bool Equals(WriteRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Equals(TableSchema, other.TableSchema) &&
                Equals(NamedObject, other.NamedObject);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            code.Add(TableSchema);
            code.Add(NamedObject);
            return code.ToHashCode();
        }

        public static bool operator ==(WriteRelation? left, WriteRelation? right)
        {
            return EqualityComparer<WriteRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(WriteRelation? left, WriteRelation? right)
        {
            return !(left == right);
        }
    }
}
