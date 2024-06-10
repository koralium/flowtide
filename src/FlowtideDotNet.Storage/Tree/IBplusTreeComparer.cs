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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    public interface IBplusTreeComparer<K, TKeyContainer>
        where TKeyContainer: IKeyContainer<K>
    {
        /// <summary>
        /// Find which index the key is in.
        /// This allows custom search algorithms that suites the storage solution.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keyContainer"></param>
        /// <returns></returns>
        int FindIndex(in K key, in TKeyContainer keyContainer);

        /// <summary>
        /// Compare two kwys with eachother
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        int CompareTo(in K x, in K y);

        /// <summary>
        /// Compare the key against the key at the index in the container
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keyContainer"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        int CompareTo(in K key, in TKeyContainer keyContainer, in int index);
    }
}
