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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal class ModifiableDeleteVector : IDeleteVector
    {
        private readonly IDeleteVector vector;
        private SortedSet<long> addedDeletes = new SortedSet<long>();

        private int countedCardinality;

        public long Cardinality
        {
            get
            {
                return countedCardinality;
            }
        }


        public ModifiableDeleteVector(IDeleteVector vector)
        {
            this.vector = vector;
        }
        public bool Contains(long index)
        {
            if (vector.Contains(index))
            {
                return true;
            }
            if (addedDeletes.Contains(index))
            {
                return true;
            }
            return false;
        }

        public void Add(long index)
        {
            addedDeletes.Add(index);
        }

        private IEnumerable<long> GetValuesInOrder()
        {
            countedCardinality = 0;
            var vectorEnumerator = vector.GetEnumerator();
            var modifiedEnumerator = addedDeletes.GetEnumerator();

            bool vectorHasNext = vectorEnumerator.MoveNext();
            bool modifiedHasNext = modifiedEnumerator.MoveNext();

            while (vectorHasNext && modifiedHasNext)
            {
                if (vectorEnumerator.Current < modifiedEnumerator.Current)
                {
                    countedCardinality++;
                    yield return vectorEnumerator.Current;
                    vectorHasNext = vectorEnumerator.MoveNext();
                }
                else
                {
                    countedCardinality++;
                    yield return modifiedEnumerator.Current;
                    modifiedHasNext = modifiedEnumerator.MoveNext();
                }
            }

            while (vectorHasNext)
            {
                countedCardinality++;
                yield return vectorEnumerator.Current;
                vectorHasNext = vectorEnumerator.MoveNext();
            }

            while (modifiedHasNext)
            {
                countedCardinality++;
                yield return modifiedEnumerator.Current;
                modifiedHasNext = modifiedEnumerator.MoveNext();
            }
        }

        public RoaringBitmapArray ToRoaringBitmapArray()
        {
            return RoaringBitmapArray.Create(GetValuesInOrder());
        }

        public IEnumerator<long> GetEnumerator()
        {
            return GetValuesInOrder().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetValuesInOrder().GetEnumerator();
        }
    }
}
