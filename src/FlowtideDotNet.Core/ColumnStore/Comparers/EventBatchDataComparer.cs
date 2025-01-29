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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    /// <summary>
    /// Comparers two event batches for equality, used for unit testing
    /// </summary>
    public class EventBatchDataComparer : IEqualityComparer<EventBatchData>
    {
        ColumnComparer columnComparer = new ColumnComparer();
        public bool Equals(EventBatchData? x, EventBatchData? y)
        {
            if (x == null && y == null)
            {
                return true;
            }
            if (x == null || y == null)
            {
                return false;
            }

            if (x.Columns.Count != y.Columns.Count)
            {
                return false;
            }

            for (int i = 0; i < x.Columns.Count; i++)
            {
                if (!columnComparer.Equals(x.Columns[i], y.Columns[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode([DisallowNull] EventBatchData obj)
        {
            HashCode hashCode = new HashCode();
            hashCode.Add(obj.Columns.Count);

            for (int i = 0; i < obj.Columns.Count; i++)
            {
                hashCode.Add(columnComparer.GetHashCode(obj.Columns[i]));
            }
            return hashCode.ToHashCode();
        }
    }
}
