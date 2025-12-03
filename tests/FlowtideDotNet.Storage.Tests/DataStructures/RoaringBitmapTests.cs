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

using FlowtideDotNet.Storage.DataStructures;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.DataStructures
{
    public class RoaringBitmapTests
    {
        [Fact]
        public void TestSimpleInsertInOrder()
        {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.Add(0);
            bitmap.Add(1);

            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(1));
            Assert.False(bitmap.Contains(2));
        }

        [Fact]
        public void TestInsertInDifferentContainers()
        {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.Add(1_000_000);
            bitmap.Add(3);

            Assert.True(bitmap.Contains(1_000_000));
            Assert.True(bitmap.Contains(3));
            Assert.False(bitmap.Contains(2));
        }

        [Fact]
        public void InsertInSameContainerOutOfOrder()
        {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.Add(4);
            bitmap.Add(1);
            bitmap.Add(9);
            bitmap.Add(7);
            bitmap.Add(72);
            bitmap.Add(63);

            Assert.True(bitmap.Contains(4));
            Assert.True(bitmap.Contains(1));
            Assert.True(bitmap.Contains(9));
            Assert.True(bitmap.Contains(7));
            Assert.True(bitmap.Contains(72));
            Assert.True(bitmap.Contains(63));

            HashSet<int> asd;
        }
    }
}
