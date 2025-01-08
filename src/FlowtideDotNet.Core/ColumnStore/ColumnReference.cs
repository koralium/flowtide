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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct ColumnReference
    {
        private IColumn column;
        private int index;
        private readonly ILockableObject? cacheObject;

        public ColumnReference(IColumn column, int index, ILockableObject? cacheObject)
        {
            this.column = column;
            this.index = index;
            this.cacheObject = cacheObject;
        }

        public void GetValue(DataValueContainer result)
        {
            column.GetValueAt(index, result, default);
        }

        public IDataValue GetValue()
        {
            return column.GetValueAt(index, default);
        }

        public void Update<T>(T value)
            where T: IDataValue
        {
            if (cacheObject != null)
            {
                cacheObject.EnterWriteLock();
                column.UpdateAt(index, value);
                cacheObject.ExitWriteLock();
            }
            else
            {
                column.UpdateAt(index, value);
            }
            
        }
    }
}
