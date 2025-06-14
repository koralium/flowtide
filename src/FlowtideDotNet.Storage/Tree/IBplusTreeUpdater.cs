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

using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    public interface IBplusTreeUpdater<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        LeafNode<K, V, TKeyContainer, TValueContainer> CurrentPage { get; }

        int CurrentIndex { get; }

        bool Found { get; }

        ValueTask Seek(in K key, IBplusTreeComparer<K, TKeyContainer>? searchComparer = null);

        public K GetKey();

        public V GetValue();

        ValueTask Delete();

        ValueTask Upsert(in K key, in V value);

        ValueTask Commit();

        ValueTask SavePage();

        void ClearCache();
    }
}
