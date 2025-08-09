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

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    internal class BoolComparer : IColumnComparer<bool>
    {
        internal static readonly BoolComparer Instance = new BoolComparer();

        public int Compare(in bool x, in bool y)
        {
            return x.CompareTo(y);
        }
    }

    internal class BoolComparerDesc : IColumnComparer<bool>
    {
        internal static readonly BoolComparerDesc Instance = new BoolComparerDesc();

        public int Compare(in bool x, in bool y)
        {
            return y.CompareTo(x);
        }
    }
}
