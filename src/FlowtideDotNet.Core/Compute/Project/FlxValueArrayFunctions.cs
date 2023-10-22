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

namespace FlowtideDotNet.Core.Compute.Project
{
    internal static class FlxValueArrayFunctions
    {
        public static FlxValue CreateArray(params FlxValue[] input)
        {
            var bytes = FlexBufferBuilder.Vector(v =>
            {
                foreach (var i in input)
                {
                    v.Add(i);
                }
            });

            return FlxValue.FromMemory(bytes);
        } 
    }
}
