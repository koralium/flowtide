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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Hints
{
    public class HintOptimizations : IEquatable<HintOptimizations>
    {
        public Dictionary<string, string> Properties { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public bool Equals(HintOptimizations? other)
        {
            if (other == null)
            {
                return false;
            }

            if (Properties.Count != other.Properties.Count)
            {
                return false;
            }

            foreach (var kvp in Properties)
            {
                if (!other.Properties.TryGetValue(kvp.Key, out var value) || !string.Equals(kvp.Value, value, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
        {
            return obj is HintOptimizations optimizations && Equals(optimizations);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach (var kvp in Properties)
            {
                code.Add(kvp.Key, StringComparer.OrdinalIgnoreCase);
                code.Add(kvp.Value, StringComparer.OrdinalIgnoreCase);
            }
            return code.ToHashCode();
        }
    }
}
