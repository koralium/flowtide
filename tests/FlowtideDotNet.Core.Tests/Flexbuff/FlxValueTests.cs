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
using System.Numerics;

namespace FlowtideDotNet.Core.Tests.Flexbuff
{
    public class FlxValueTests
    {
        [Fact]
        public void TestAddFlxValueString()
        {
            string vall = "test";
            var val = FlxValue.FromBytes(FlexBuffer.SingleValue(vall));

            System.Diagnostics.Stopwatch stopwatch2 = new System.Diagnostics.Stopwatch();
            
            var o = FlexBufferBuilder.Vector(b =>
            {
                b.Add(1);
                stopwatch2.Start();
                for (int i = 0; i < 1; i++)
                {
                    b.Add(vall);
                }
                stopwatch2.Stop();
            });
            

            System.Diagnostics.Stopwatch stopwatch1 = new System.Diagnostics.Stopwatch();
            
            var b = FlexBufferBuilder.Vector(b =>
            {
                b.Add(1);
                //b.Add("hejsan");
                stopwatch1.Start();
                for (int i = 0; i < 1; i++)
                {
                    b.Add(val);
                }
                stopwatch1.Stop();
            });
            

            var v = FlxValue.FromBytes(b);
            var vv = v.AsVector;
            var stringVal = vv[1].AsString;
        }

        [Fact]
        public void TestBitOp()
        {
            var packedType = 0;
            var v = (1 << (packedType & 3));

            var trailing = BitOperations.TrailingZeroCount(v);
            var value = v;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            var vv = v >> 1;
        }
    }
}
