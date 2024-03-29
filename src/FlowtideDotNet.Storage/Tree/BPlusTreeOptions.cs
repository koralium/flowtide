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

namespace FlowtideDotNet.Storage.Tree
{
    public class BPlusTreeOptions<K, V>
    {
        /// <summary>
        /// Override the default page size. This should only be set if the operator works best with a specific size.
        /// </summary>
        public int? BucketSize { get; set; }

        public required IComparer<K> Comparer { get; set; }

        /// <summary>
        /// Serializer for the keys
        /// </summary>
        public required IBplusTreeSerializer<K> KeySerializer { get; set; }

        /// <summary>
        /// Serializer for values
        /// </summary>
        public required IBplusTreeSerializer<V> ValueSerializer { get; set; }
    }
}
