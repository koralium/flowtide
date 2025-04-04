﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Storage.AppendTree;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Storage
{
    public interface IAppendTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
    {
        ValueTask Append(in K key, in V value);
        ValueTask Commit();
        ValueTask Prune(K key);
        internal Task<string> Print();
        IAppendTreeIterator<K, V, TKeyContainer> CreateIterator();
        ValueTask Clear();
    }
}
