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

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    internal ref struct TypeIntStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public TypeIntStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public int BitWidth 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 4); 
                return o != 0 ? ReadUtils.GetInt(in span, o + position) : (int)0; 
            } 
        }

        public bool IsSigned 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 6); 
                return o != 0 ? 0 != ReadUtils.Get(in span, o + position) : (bool)false; 
            } 
        }
    }
}
