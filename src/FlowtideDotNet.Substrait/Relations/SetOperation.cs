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

namespace FlowtideDotNet.Substrait.Relations
{
    public enum SetOperation
    {
        Unspecified = 0,
        MinusPrimary = 1,
        MinusMultiset = 2,
        MinusPrimaryAll = 7,
        IntersectionPrimary = 3,
        IntersectionMultiset = 4,
        IntersectionMultisetAll = 8,
        UnionDistinct = 5,
        UnionAll = 6,
    }
}
