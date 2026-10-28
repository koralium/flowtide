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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowStructComparer : IArrowComparer
    {
        private readonly List<IArrowComparer> _propertyComparers;
        private readonly FieldsCache _fields = new FieldsCache();

        public ArrowStructComparer(List<IArrowComparer> propertyComparers)
        {
            this._propertyComparers = propertyComparers;
        }

        // Validity first, a null struct ends the comparison
        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (StructArray)array;
            var right = (StructArray)otherArray;

            if (left.IsNull(leftIndex) || right.IsNull(rightIndex))
            {
                return left.IsNull(leftIndex) && right.IsNull(rightIndex);
            }
            var leftFields = _fields.Get(left);
            var rightFields = _fields.Get(right);
            for (int i = 0; i < _propertyComparers.Count; i++)
            {
                if (!_propertyComparers[i].IsEqual(leftIndex, leftFields[i], rightIndex, rightFields[i]))
                {
                    return false;
                }
            }

            return true;
        }

        // A null struct skips its fields
        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            if (ArrowHash.IsNull(array, index))
            {
                ArrowHash.Add(ref state, ArrowHash.Null);
                return true;
            }
            var fields = _fields.Get((StructArray)array!);
            for (int i = 0; i < _propertyComparers.Count; i++)
            {
                if (!_propertyComparers[i].TryHash(fields[i], index, ref state))
                {
                    return false;
                }
            }
            return true;
        }

        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            var nulls = ArrowHash.CompareNulls(left, leftIndex, right, rightIndex);
            if (nulls != null)
            {
                return nulls.Value;
            }
            var leftFields = _fields.Get((StructArray)left!);
            var rightFields = _fields.Get((StructArray)right!);
            for (int i = 0; i < _propertyComparers.Count; i++)
            {
                var compared = _propertyComparers[i].CompareCanonical(leftFields[i], leftIndex, rightFields[i], rightIndex);
                if (compared != 0)
                {
                    return compared;
                }
            }
            return 0;
        }
    }
}
