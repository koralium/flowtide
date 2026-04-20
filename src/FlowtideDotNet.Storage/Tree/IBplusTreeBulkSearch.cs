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
    public struct BulkSearchKeyResult
    {
        /// <summary>
        /// The original index of the search key in the input array.
        /// </summary>
        public int KeyIndex;

        /// <summary>
        /// The lower bound index in the leaf. Negative (bitwise complement) if the key was not found.
        /// </summary>
        public int LowerBound;

        /// <summary>
        /// The upper bound index in the leaf. Negative (bitwise complement) if the key was not found.
        /// </summary>
        public int UpperBound;

        /// <summary>
        /// True if this key may continue into the next leaf (upper bound is the last index of the leaf).
        /// </summary>
        public bool ContinuesToNextLeaf;

        public bool Found => LowerBound >= 0;
    }

    public interface IBplusTreeBulkSearch<K, V, TKeyContainer, TValueContainer, TComparer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
        where TComparer : IBplusTreeComparer<K, TKeyContainer>
    {
        ValueTask Start(K[] keys, int keyLength);

        ValueTask Start(K[] keys, int keyLength, int[] sortedIndices);

        ValueTask<bool> MoveNextLeaf();

        LeafNode<K, V, TKeyContainer, TValueContainer> CurrentLeaf { get; }

        IReadOnlyList<BulkSearchKeyResult> CurrentResults { get; }
    }
}
