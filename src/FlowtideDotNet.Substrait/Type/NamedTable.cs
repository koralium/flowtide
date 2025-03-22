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


namespace FlowtideDotNet.Substrait.Type
{
    public sealed class NamedTable : IEquatable<NamedTable>
    {
        public required List<string> Names { get; set; }

        public string DotSeperated
        {
            get
            {
                return string.Join(".", Names);
            }
        }

        public override bool Equals(object? obj)
        {
            return obj is NamedTable table &&
                   Equals(table);
        }

        public bool Equals(NamedTable? other)
        {
            return other != null &&
                   Names.SequenceEqual(other.Names);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach (var name in Names)
            {
                code.Add(name);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(NamedTable? left, NamedTable? right)
        {
            return EqualityComparer<NamedTable>.Default.Equals(left, right);
        }

        public static bool operator !=(NamedTable? left, NamedTable? right)
        {
            return !(left == right);
        }
    }
}
