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

using System.Text;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal abstract class BaseNode<K> : IBPlusTreeNode
    {
        public List<K> keys;

        public BaseNode(long id)
        {
            keys = new List<K>();
            Id = id;
        }

        public long Id { get; }

        public void EnterWriteLock()
        {
            Monitor.Enter(this);
        }

        public void ExitWriteLock()
        {
            Monitor.Exit(this);
        }

        public abstract Task Print(StringBuilder stringBuilder, Func<long, ValueTask<BaseNode<K>>> lookupFunc);

        public abstract Task PrintNextPointers(StringBuilder stringBuilder);
    }
}
