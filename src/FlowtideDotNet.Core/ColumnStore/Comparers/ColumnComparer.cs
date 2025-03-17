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

using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    /// <summary>
    /// Checks equality of two columns, used for unit testing
    /// </summary>
    internal class ColumnComparer : IEqualityComparer<IColumn>
    {
        DataValueComparer dataValueComparer = new DataValueComparer();

        public bool Equals(IColumn? x, IColumn? y)
        {
            if (x == null && y == null)
            {
                return true;
            }
            if (x == null || y == null)
            {
                return false;
            }
            if (x.Count != y.Count)
            {
                return false;
            }
            
            for (int i = 0; i < x.Count; i++)
            {
                var xVal = x.GetValueAt(i, default);
                var yVal = y.GetValueAt(i, default);
                if (dataValueComparer.Compare(xVal, yVal) != 0)
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode([DisallowNull] IColumn obj)
        {
            // Will be fixed later
            return 1;
        }
    }
}
