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

using FlowtideDotNet.Storage.DataStructures.RoaringBitMap;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace FlowtideDotNet.Storage.DataStructures
{
    internal class ArrayContainer : Container
    {
        internal ushort[] content;
        internal int cardinality;

        internal const int DEFAULT_INIT_SIZE = 4;
        internal const int DEFAULT_MAX_SIZE = 4096;

        public override bool IsEmpty => cardinality == 0;

        public ArrayContainer()
            : this(DEFAULT_INIT_SIZE)
        {
            
        }

        public ArrayContainer(int capacity)
        {
            content = new ushort[capacity];
        }

        public ArrayContainer(ushort[] content, int cardinality)
        {
            this.content = content;
            this.cardinality = cardinality;
        }

        public override Container Add(ushort x)
        {
            if (cardinality == 0 || (cardinality > 0 && (x) > (content[cardinality - 1])))
            {
                if (cardinality >= DEFAULT_MAX_SIZE)
                {
                    return ToBitmapContainer().Add(x);
                }
                if (cardinality >= this.content.Length)
                {
                    IncreaseCapacity();
                }
                content[cardinality++] = x;
            }
            else
            {
                int loc = Utils.UnsignedBinarySearch(content, 0, cardinality, x);
                if (loc < 0)
                {
                    // Transform the ArrayContainer to a BitmapContainer
                    // when cardinality = DEFAULT_MAX_SIZE
                    if (cardinality >= DEFAULT_MAX_SIZE)
                    {
                        return ToBitmapContainer().Add(x);
                    }
                    if (cardinality >= this.content.Length)
                    {
                        IncreaseCapacity();
                    }
                    // insertion : shift the elements > x by one position to
                    // the right
                    // and put x in it's appropriate place

                    // Array copy
                    loc = ~loc;
                    Array.Copy(content, loc, content, loc + 1, cardinality - loc);
                    content[loc] = x;
                    ++cardinality;
                }
            }
            return this;
        }

        public BitmapContainer ToBitmapContainer()
        {
            BitmapContainer bc = new BitmapContainer();
            bc.LoadData(this);
            return bc;
        }

        private void IncreaseCapacity()
        {
            IncreaseCapacity(false);
        }

        private void IncreaseCapacity(bool allowIllegalSize)
        {
            int newCapacity = ComputeCapacity(this.content.Length);
            // never allocate more than we will ever need
            if (newCapacity > DEFAULT_MAX_SIZE && !allowIllegalSize)
            {
                newCapacity = DEFAULT_MAX_SIZE;
            }
            // if we are within 1/16th of the max, go to max
            if (newCapacity > DEFAULT_MAX_SIZE - DEFAULT_MAX_SIZE / 16
                && !allowIllegalSize)
            {
                newCapacity = DEFAULT_MAX_SIZE;
            }

            var newArr = new ushort[newCapacity];
            Array.Copy(this.content, 0, newArr, 0, this.content.Length);
            this.content = newArr;
        }

        private int ComputeCapacity(int oldCapacity)
        {
            return oldCapacity == 0
                ? DEFAULT_INIT_SIZE
                : oldCapacity < 64
                    ? oldCapacity * 2
                    : oldCapacity < 1024 ? oldCapacity * 3 / 2 : oldCapacity * 5 / 4;
        }

        public override bool Contains(ushort x)
        {
            return Utils.UnsignedBinarySearch(content, 0, cardinality, x) >= 0;
        }

        public override Container Remove(ushort x)
        {
            var loc = Utils.UnsignedBinarySearch(content, 0, cardinality, x);
            if (loc >= 0)
            {
                RemoveAtIndex(loc);
            }
            return this;
        }

        private void RemoveAtIndex(int loc)
        {
            Array.Copy(content, loc + 1, content, loc, cardinality - loc - 1);
            --cardinality;
        }

        internal void LoadData(BitmapContainer bitmapContainer)
        {
            this.cardinality = bitmapContainer.cardinality;
            Utils.FillArray(bitmapContainer.bitmap, content);
        }

        public override int GetCardinality()
        {
            return cardinality;
        }

        public override IEnumerator<ushort> GetEnumerator()
        {
            return new ArrayContainerEnumerator(this);
        }

        public override IContainerEnumerator GetContainerEnumerator()
        {
            return new ArrayContainerEnumerator(this);
        }
    }
}
