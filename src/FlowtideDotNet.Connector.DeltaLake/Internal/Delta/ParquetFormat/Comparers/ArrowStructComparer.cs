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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowStructComparer : IArrowComparer
    {
        private readonly List<IArrowComparer> _propertyComparers;

        public ArrowStructComparer(List<IArrowComparer> propertyComparers)
        {
            this._propertyComparers = propertyComparers;
        }

        public int FindOccurance(int toFindIndex, IArrowArray toFindFrom, int searchIndex, int searchLength, IArrowArray toFindIn, int globalOffset, IDeleteVector deleteVector)
        {
            for (int i = searchIndex; i < searchLength; i++)
            {
                if (deleteVector.Contains(globalOffset + i))
                {
                    continue;
                }
                if (IsEqual(toFindIndex, toFindFrom, i, toFindIn))
                {
                    return i;
                }
            }
            return -1;
        }

        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (StructArray)array;
            var right = (StructArray)otherArray;

            for (int i = 0; i < _propertyComparers.Count; i++)
            {
                if (!_propertyComparers[i].IsEqual(leftIndex, left.Fields[i], rightIndex, right.Fields[i]))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
