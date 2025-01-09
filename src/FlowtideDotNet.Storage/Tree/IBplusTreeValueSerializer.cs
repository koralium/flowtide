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
    public interface IBplusTreeValueSerializer<V, TValueContainer>
        where TValueContainer: IValueContainer<V>
    {
        TValueContainer CreateEmpty();

        TValueContainer Deserialize(in BinaryReader reader);

        void Serialize(in BinaryWriter writer, in TValueContainer values);

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
