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

using System.Buffers;

namespace FlowtideDotNet.Storage.Tree
{
    public interface IBPlusTreeKeySerializer<K, TKeyContainer>
        where TKeyContainer : IKeyContainer<K>
    {
        TKeyContainer CreateEmpty();

        TKeyContainer Deserialize(ref SequenceReader<byte> reader);

        void Serialize(in IBufferWriter<byte> writer, in TKeyContainer values);

        /// <summary>
        /// Called on each checkpoint, its primary function is to allow a serializer to write any potential metadata
        /// such as schema information, global dictionaries and similar.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context);

        /// <summary>
        /// Called once during the initialization of the serializer.
        /// This allows the serializer to fetch metadata pages such as schema information, global dictionaries and similar.
        /// During initialize any in memory data should be cleared and refetched since it can be called after a restore from
        /// a previous checkpoint.
        /// </summary>
        /// <param name="context">Context that allows reading pages and also allocating new page ids.</param>
        /// <returns></returns>
        Task InitializeAsync(IBPlusTreeSerializerInitializeContext context);
    }
}
