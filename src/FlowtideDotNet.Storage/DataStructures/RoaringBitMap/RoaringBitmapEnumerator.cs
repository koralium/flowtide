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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DataStructures.RoaringBitMap
{
    internal class RoaringBitmapEnumerator : IEnumerator<int>
    {
        private readonly RoaringBitmap roaringBitmap;
        private ushort startingContainerIndex;
        private int pos = 0;
        private IContainerEnumerator iter;
        private int hs = 0;

        public RoaringBitmapEnumerator(RoaringBitmap roaringBitmap)
        {
            this.roaringBitmap = roaringBitmap;
            startingContainerIndex = FindStartingContainerIndex();
            NextContainer();
        }

        public int Current { get; set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        private bool HasNext()
        {
            return pos < roaringBitmap.highLowContainer.Size;
        }

        [MemberNotNull(nameof(iter))]
        private void NextContainer()
        {
            int containerSize = roaringBitmap.highLowContainer.Size;
            if (pos < containerSize)
            {
                int index = (pos + startingContainerIndex) % containerSize;
                iter = roaringBitmap.highLowContainer.GetContainerAtIndex(index).GetContainerEnumerator();
                hs = roaringBitmap.highLowContainer.GetKeyAtIndex(index) << 16;
            }
        }

        private int Next()
        {
            int x = iter.Next() | hs;
            if (!iter.HasNext())
            {
                ++pos;
                NextContainer();
            }
            return x;
        }

        public bool MoveNext()
        {
            if (HasNext()) 
            {
                Current = Next();
                return true;
            }
            return false;
        }

        public void Reset()
        {
            pos = 0;
            startingContainerIndex = FindStartingContainerIndex();
            NextContainer();
        }

        ushort FindStartingContainerIndex()
        {
            return 0;
        }
    }
}
