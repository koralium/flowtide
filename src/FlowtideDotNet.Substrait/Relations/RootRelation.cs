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


namespace FlowtideDotNet.Substrait.Relations
{
    public class RootRelation : Relation
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
            if (obj is RootRelation relation)
            {
                return base.Equals(relation) &&
                   Equals(Input, relation.Input) &&
                   Names.SequenceEqual(relation.Names);
            }
            return false;
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            foreach(var name in Names)
            {
                code.Add(name);
            }
            return code.ToHashCode();
        }
    }
}
