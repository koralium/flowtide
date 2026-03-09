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

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    internal class GuidComparer : IColumnComparer<Guid>
    {
        internal static readonly GuidComparer Instance = new GuidComparer();
        public int Compare(in Guid x, in Guid y)
        {
            return x.CompareTo(y);
        }
    }

    internal class GuidComparerDesc : IColumnComparer<Guid>
    {
        internal static readonly GuidComparerDesc Instance = new GuidComparerDesc();

        public int Compare(in Guid x, in Guid y)
        {
            return y.CompareTo(x);
        }
    }
}
