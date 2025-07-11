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

using FlowtideDotNet.Core.ColumnStore.Utils;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Utils
{
    public class FindFirstOccurenceTests
    {

        [Fact]
        public void FindFirstOccurenceFirstCheck()
        {

            var array = new sbyte[255];

            for (int i = 0; i < 255; i++)
            {
                array[i] = (sbyte)i;
            }

            sbyte value = 5;
            var result = AvxUtils.FindFirstOccurence(array, 0, value);
            Assert.Equal(5, result);
        }

        [Fact]
        public void FindFirstOccurenceAfterLoop()
        {

            var array = new sbyte[255];

            for (int i = 0; i < 255; i++)
            {
                array[i] = (sbyte)i;
            }

            sbyte value = 125;
            var result = AvxUtils.FindFirstOccurence(array, 0, value);
            Assert.Equal(125, result);
        }

        [Fact]
        public void FindFirstOccurenceNonExisting()
        {
            var array = new sbyte[255];

            for (int i = 0; i < 255; i++)
            {
                array[i] = (sbyte)1;
            }

            sbyte value = 5;
            var result = AvxUtils.FindFirstOccurence(array, 0, value);
            Assert.Equal(-1, result);
        }
    }
}
