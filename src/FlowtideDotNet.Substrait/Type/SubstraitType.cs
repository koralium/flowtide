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

namespace FlowtideDotNet.Substrait.Type
{
    public enum SubstraitType
    {
        String = 0,
        Int32 = 1,
        Any = 2,
        Date = 3,
        Fp64 = 4,
        Int64 = 5,
        Bool = 6,
        Fp32 = 7,
        Decimal = 8,
        Struct = 9,
        Map = 10,
        List = 11,
        Binary = 12,
        TimestampTz = 13
    }
}
