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
    internal class Int16Comparer : IColumnComparer<short>
    {
        internal static readonly Int16Comparer Instance = new Int16Comparer();

        public int Compare(in short x, in short y)
        {
            return x.CompareTo(y);
        }
    }

    internal class Int16ComparerDesc : IColumnComparer<short>
    {
        internal static readonly Int16ComparerDesc Instance = new Int16ComparerDesc();

        public int Compare(in short x, in short y)
        {
            return y.CompareTo(x);
        }
    }
}
