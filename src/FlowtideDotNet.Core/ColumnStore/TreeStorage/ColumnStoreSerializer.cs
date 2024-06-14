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

using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class ColumnStoreSerializer : IBPlusTreeKeySerializer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly int columnCount;

        public ColumnStoreSerializer(int columnCount)
        {
            this.columnCount = columnCount;
        }
        public ColumnKeyStorageContainer CreateEmpty()
        {
            return new ColumnKeyStorageContainer(columnCount);
        }

        public ColumnKeyStorageContainer Deserialize(in BinaryReader reader)
        {
            throw new NotImplementedException();
        }

        public void Serialize(in BinaryWriter writer, in ColumnKeyStorageContainer values)
        {
            
            throw new NotImplementedException();
        }
    }
}
