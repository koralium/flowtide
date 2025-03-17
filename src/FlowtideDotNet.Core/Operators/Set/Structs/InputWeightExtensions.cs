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

namespace FlowtideDotNet.Core.Operators.Set.Structs
{
    internal static class InputWeightExtensions
    {
        public static void Add<TStruct>(ref TStruct inputWeight, TStruct other)
            where TStruct : IInputWeight
        {
            for (int i = 0; i < inputWeight.Count; i++)
            {
                int value = inputWeight.GetValue(i) + other.GetValue(i);
                inputWeight.SetValue(i, value);
            }
        }
    }
}
