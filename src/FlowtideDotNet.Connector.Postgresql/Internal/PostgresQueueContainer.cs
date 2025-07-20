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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    internal class PostgresQueueContainer : IValueContainer<ColumnRowReference>
    {
        public int Count => throw new NotImplementedException();

        public void AddRangeFrom(IValueContainer<ColumnRowReference> container, int start, int count)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public ColumnRowReference Get(int index)
        {
            throw new NotImplementedException();
        }

        public int GetByteSize()
        {
            throw new NotImplementedException();
        }

        public int GetByteSize(int start, int end)
        {
            throw new NotImplementedException();
        }

        public ref ColumnRowReference GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, ColumnRowReference value)
        {
            throw new NotImplementedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotImplementedException();
        }

        public void RemoveRange(int start, int count)
        {
            throw new NotImplementedException();
        }

        public void Update(int index, ColumnRowReference value)
        {
            throw new NotImplementedException();
        }
    }
}
