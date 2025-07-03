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

namespace FlowtideDotNet.Base
{
    public abstract class AbstractWatermarkValue : IComparable<AbstractWatermarkValue>
    {
        public abstract int TypeId { get; }

        public long BatchID { get; set; }

        public static AbstractWatermarkValue? Min(AbstractWatermarkValue? a, AbstractWatermarkValue? b)
        {
            if (a == null || b == null)
            {
                return null;
            }
            return a.CompareTo(b) < 0 ? a : b;
        }

        public abstract int Compare(AbstractWatermarkValue? other);

        public int CompareTo(AbstractWatermarkValue? other)
        {
            if (other == null)
            {
                return 1; // This instance is greater than null
            }
            if (this.TypeId != other.TypeId)
            {
                throw new ArgumentException("Cannot compare different types of AbstractWatermarkValue");
            }
            var result = Compare(other);
            if (result != 0)
            {
                return result;
            }
            return BatchID.CompareTo(other.BatchID);
        }
    }
}
