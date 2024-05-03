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


namespace FlowtideDotNet.Substrait.Expressions
{
    public class StructReferenceSegment : ReferenceSegment, IEquatable<StructReferenceSegment>
    {
        public int Field { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is StructReferenceSegment segment &&
                   Equals(segment);
        }

        public bool Equals(StructReferenceSegment? other)
        {
            return other != null &&
                   Equals(Child, other.Child) &&
                   Field == other.Field;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Child, Field);
        }

        public static bool operator ==(StructReferenceSegment? left, StructReferenceSegment? right)
        {
            return EqualityComparer<StructReferenceSegment>.Default.Equals(left, right);
        }

        public static bool operator !=(StructReferenceSegment? left, StructReferenceSegment? right)
        {
            return !(left == right);
        }
    }
}
