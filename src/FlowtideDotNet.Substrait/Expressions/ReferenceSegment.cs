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
    public abstract class ReferenceSegment : IEquatable<ReferenceSegment>
    {
        public ReferenceSegment? Child { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is ReferenceSegment segment &&
                   Equals(segment);
        }

        public virtual bool Equals(ReferenceSegment? other)
        {
            return other != null &&
                   Equals(Child, other.Child);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Child);
        }

        public static bool operator ==(ReferenceSegment? left, ReferenceSegment? right)
        {
            return EqualityComparer<ReferenceSegment>.Default.Equals(left, right);
        }

        public static bool operator !=(ReferenceSegment? left, ReferenceSegment? right)
        {
            return !(left == right);
        }
    }
}
