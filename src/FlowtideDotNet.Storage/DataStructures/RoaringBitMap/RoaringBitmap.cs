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
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DataStructures
{
    internal class RoaringBitmap : IEnumerable<int>
    {
        internal RoaringArray highLowContainer;

        public RoaringBitmap()
        {
            highLowContainer = new RoaringArray();
        }

        public bool this[int index]
        {
            get
            {
                return Contains(index);
            }
            set
            {
                if (value)
                {
                    Add(index);
                }
                else
                {
                    Remove(index);
                }
            }
        }

        public int Count => GetCardinality();

        public int GetCardinality()
        {
            int size = 0;
            for (int i = 0; i < this.highLowContainer.Size; i++)
            {
                size += this.highLowContainer.GetContainerAtIndex(i).GetCardinality();
            }
            return size;
        }

        public void Add(int x)
        {
            var hb = Utils.HighBits(x);
            var index = highLowContainer.GetIndex(hb);

            if (index >= 0)
            {
                var container = highLowContainer.GetContainerAtIndex(index);
                var newContainer = container.Add(Utils.LowBits(x));
                highLowContainer.SetContainerAtIndex(index, newContainer);
            }
            else
            {
                ArrayContainer newac = new ArrayContainer();
                highLowContainer.InsertNewKeyValueAt(~index, hb, newac.Add(Utils.LowBits(x)));
            }
        }

        public bool Contains(int x)
        {
            ushort hb = Utils.HighBits(x);
            int index = highLowContainer.GetContainerIndex(hb);
            if (index < 0)
            {
                return false;
            }
            var c = highLowContainer.GetContainerAtIndex(index);
            return c.Contains(Utils.LowBits(x));
        }

        public IEnumerator<int> GetEnumerator()
        {
            return new RoaringBitmapEnumerator(this);
        }

        public void Remove(int x)
        {
            ushort hb = Utils.HighBits(x);
            int index = highLowContainer.GetContainerIndex(hb);
            if (index < 0)
            {
                return;
            }
            var c = highLowContainer.GetContainerAtIndex(index);
            var newContainer = c.Remove(Utils.LowBits(x));
            if (newContainer.IsEmpty)
            {
                highLowContainer.RemoveAtIndex(index);
            }
            else
            {
                highLowContainer.SetContainerAtIndex(index, newContainer);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Serialize(IBufferWriter<byte> writer)
        {

        }
    }
}
