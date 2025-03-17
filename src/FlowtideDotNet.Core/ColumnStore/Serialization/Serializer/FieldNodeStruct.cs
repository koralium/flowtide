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

using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref struct FieldNodeStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public FieldNodeStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length 
        { 
            get 
            { 
                return ReadUtils.GetLong(in span, position + 0); 
            } 
        }

        public long NullCount 
        { 
            get 
            { 
                return ReadUtils.GetLong(in span, position + 8); 
            } 
        }
    }
}
