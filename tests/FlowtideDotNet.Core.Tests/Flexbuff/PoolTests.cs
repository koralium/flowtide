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

using FlexBuffers;
using FluentAssertions;

namespace FlowtideDotNet.Core.Tests.Flexbuff
{
    public class PoolTests
    {

        private static byte[] FillData(FlexBuffer flexBuffer)
        {
            flexBuffer.NewObject();
            var vecStart = flexBuffer.StartVector();
            flexBuffer.Add(3213);
            flexBuffer.Add(true);
            flexBuffer.Add("hello world");
            flexBuffer.Add(1);
            flexBuffer.Add(123.0f);
            flexBuffer.Add("What is happening, hello all, this is some longer text");
            flexBuffer.EndVector(vecStart, false, false);
            return flexBuffer.Finish();
        }

        [Fact]
        public void TestDirtyArray()
        {
            var flexBuffClean = new FlexBuffer(new CleanArrayPool());
            var flexBuffDirt = new FlexBuffer(new DirtyArrayPool());

            var cleanArray = FillData(flexBuffClean);
            var dirtyArray = FillData(flexBuffDirt);

            dirtyArray.Should().Equal(cleanArray);
        }
    }
}
