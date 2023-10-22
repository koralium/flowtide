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

namespace FlowtideDotNet.Core.FlexFast
{
    internal enum FlexType : byte
    {
        Null, Int, Uint, Float,
        Key, String, IndirectInt, IndirectUInt, IndirectFloat,
        Map, Vector, VectorInt, VectorUInt, VectorFloat, VectorKey, VectorString,
        VectorInt2, VectorUInt2, VectorFloat2,
        VectorInt3, VectorUInt3, VectorFloat3,
        VectorInt4, VectorUInt4, VectorFloat4,
        Blob, Bool, VectorBool = 36
    }
}
