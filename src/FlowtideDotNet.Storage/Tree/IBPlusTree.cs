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
    public enum GenericWriteOperation
    {
        None = 0,
        /// <summary>
        /// Value is updated
        /// </summary>
        Upsert = 1,
        /// <summary>
        /// Value is deleted
        /// </summary>
        Delete = 2
    }

    public delegate (V? result, GenericWriteOperation operation) GenericWriteFunction<V>(V? input, V? current, bool exists);

    public interface IBPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer: IKeyContainer<K>
        where TValueContainer: IValueContainer<V>
    {
        ValueTask Upsert(in K key, in V value);

        ValueTask Delete(in K key);

        ValueTask<GenericWriteOperation> RMWNoResult(in K key, in V? value, in GenericWriteFunction<V> function);

        ValueTask<(GenericWriteOperation operation, V? result)> RMW(in K key, in V? value, in GenericWriteFunction<V> function);

        ValueTask<(bool found, V? value)> GetValue(in K key);

        ValueTask<(bool found, K? key)> GetKey(in K key);

        IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> CreateIterator();

        /// <summary>
        /// For debugging purposes only
        /// </summary>
        /// <returns></returns>
        internal Task<string> Print();

        ValueTask Commit();

        /// <summary>
        /// Clears the tree and returns a blank new tree.
        /// 
        /// This is useful for temporary data storage which will offload to disk when the size grows, but can be deleted on demand.
        /// </summary>
        /// <returns></returns>
        ValueTask Clear();

        long CacheMisses { get; }
    }
}
