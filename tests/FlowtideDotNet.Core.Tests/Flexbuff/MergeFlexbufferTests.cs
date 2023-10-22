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

using FlowtideDotNet.Core.Flexbuffer;
using FlexBuffers;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.Flexbuff
{
    public class MergeFlexbufferTests
    {
        [Fact]
        public void TestMergeTwoBuffers()
        {
            var asdad = typeof(System.Runtime.InteropServices.PosixSignal).Assembly.GetTypes().OrderBy(x => x.Name).Select(x => x.Name).ToList();

            var ssss = asdad.Where(x => x.Contains("Sys")).ToList();
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            var interopClass = typeof(int).Assembly.GetTypes().FirstOrDefault(x => x.Name == "Interop");
            var sysClass = interopClass.GetNestedTypes(BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Public | BindingFlags.Instance);
            //var methods = sysClass.GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);

            var buff1 = FlexBuffers.FlexBufferBuilder.Vector(c =>
            {
                c.Add(1);
                c.Add("hello1");
            });
            var buff2 = FlexBuffers.FlexBufferBuilder.Vector(c =>
            {
                c.Add(2);
                c.Add("hello2");
            });

            var vec1 = FlxValue.FromBytes(buff1).AsVector;
            var vec2 = FlxValue.FromBytes(buff2).AsVector;

            FlexbufferMerger.Merge(vec1, vec2);

            
            //System.Runtime.InteropServices.RuntimeEnvironment.
        }

        [Fact]
        public void TestMergeTwoBuffersWidth2()
        {
            var buff1 = FlexBuffers.FlexBufferBuilder.Vector(c =>
            {
                c.Add(512);
                c.Add(17);
                c.Add("hello1");
            });
            var buff2 = FlexBuffers.FlexBufferBuilder.Vector(c =>
            {
                c.Add(513);
                c.Add("hello2");
            });

            var vec1 = FlxValue.FromBytes(buff1).AsVector;
            var vec2 = FlxValue.FromBytes(buff2).AsVector;

            FlexbufferMerger.Merge(vec1, vec2);
        }
    }
}
