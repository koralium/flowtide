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


using Substrait.Protobuf;

namespace FlowtideDotNet.Substrait.Type
{
    public class VirtualTable : IEquatable<VirtualTable>
    {
        public List<string> JsonValues { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is VirtualTable table &&
                   Equals(table);
        }

        public bool Equals(VirtualTable? other)
        {
            return other != null &&
                   JsonValues.SequenceEqual(other.JsonValues);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach (var value in JsonValues)
            {
                code.Add(value);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(VirtualTable? left, VirtualTable? right)
        {
            return EqualityComparer<VirtualTable>.Default.Equals(left, right);
        }

        public static bool operator !=(VirtualTable? left, VirtualTable? right)
        {
            return !(left == right);
        }
    }
}
