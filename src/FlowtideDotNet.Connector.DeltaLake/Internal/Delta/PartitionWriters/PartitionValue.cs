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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.PartitionWriters
{
    internal class PartitionValue : IEquatable<PartitionValue>
    {
        public static readonly PartitionValue Empty = new PartitionValue(new List<KeyValuePair<string, string>>());

        public List<KeyValuePair<string, string>> _values;

        public PartitionValue(List<KeyValuePair<string, string>> values)
        {
            _values = values;
        }

        public bool Equals(PartitionValue? other)
        {
            if (other == null)
            {
                return false;
            }

            if (_values.Count != other._values.Count)
            {
                return false;
            }

            for (int i = 0; i < _values.Count; i++)
            {
                if (_values[i].Key != other._values[i].Key || _values[i].Value != other._values[i].Value)
                {
                    return false;
                }
            }

            return true;
        }

        public override int GetHashCode()
        {
            HashCode hashCode = new HashCode();

            foreach (var value in _values)
            {
                hashCode.Add(value.Key);
                hashCode.Add(value.Value);
            }

            return hashCode.ToHashCode();
        }
    }
}
