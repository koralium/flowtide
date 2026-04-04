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

namespace FlowtideDotNet.Substrait.Hints
{
    public class Hint : IEquatable<Hint>
    {
        public string? Alias { get; set; }

        public HintOptimizations Optimizations { get; set; } = new HintOptimizations();

        public bool HasHints
        {
            get
            {
                return Alias != null ||
                    Optimizations.Properties.Count > 0;
            }
        }

        public bool Equals(Hint? other)
        {
            return other != null &&
                string.Equals(Alias, other.Alias, StringComparison.OrdinalIgnoreCase) &&
                Optimizations.Equals(other.Optimizations);
        }

        public override bool Equals(object? obj)
        {
            return obj is Hint hint && Equals(hint);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Alias, StringComparer.OrdinalIgnoreCase);
            code.Add(Optimizations);
            return code.ToHashCode();
        }

        public Hint Clone()
        {
            return new Hint
            {
                Alias = this.Alias,
                Optimizations = this.Optimizations.Clone()
            };
        }
    }
}
