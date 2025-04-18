﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.ColumnStore.DataValues;

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    internal class TimestampTzComparer : IColumnComparer<TimestampTzValue>
    {
        internal static readonly TimestampTzComparer Instance = new TimestampTzComparer();
        public int Compare(in TimestampTzValue x, in TimestampTzValue y)
        {
            return x.CompareTo(y);
        }
    }

    internal class TimestampTzComparerDesc : IColumnComparer<TimestampTzValue>
    {
        internal static readonly TimestampTzComparerDesc Instance = new TimestampTzComparerDesc();
        public int Compare(in TimestampTzValue x, in TimestampTzValue y)
        {
            return y.CompareTo(x);
        }
    }
}
