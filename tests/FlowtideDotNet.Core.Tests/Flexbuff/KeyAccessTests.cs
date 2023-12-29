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
using FlowtideDotNet.Core.Flexbuffer;
using System.Text;

namespace FlowtideDotNet.Core.Tests.Flexbuff
{
    /// <summary>
    /// Tests for accessing keys in a map
    /// </summary>
    public class KeyAccessTests
    {
        [Fact]
        public void AccessAsFlxString()
        {
            var bytes = FlexBufferBuilder.Map(m =>
            {
                m.Add("test", 543);
                m.Add("test2", 123);
            });
            var val = FlxValueRef.FromSpan(bytes);
            var vector = val.AsMap;
            var v = vector.Keys[0].AsFlxString;
            var actual = Encoding.UTF8.GetString(v.Span);
            Assert.Equal("test", actual);
        }
    }
}
