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

using FlowtideDotNet.Storage;

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    public class QdrantSinkState
    {
        /// <summary>
        /// The name of the collection, this is inherited from <see cref="QdrantSinkOptions"/> and can be set to a new collection name.
        /// This value will be used when making requests against Qdrant.
        /// </summary>
        public required string CollectionName { get; set; }

        public required string OriginalCollectionName { get; init; }

        /// <summary>
        /// Version information for the stream.
        /// </summary>
        public StreamVersionInformation? StreamVersion { get; set; }

        /// <summary>
        /// When the last checkpoint occured.
        /// </summary>
        public DateTimeOffset LastCheckpointTime { get; set; }

        /// <summary>
        /// When the last update was done.
        /// </summary>
        public DateTimeOffset LastUpdate { get; set; }

        /// <summary>
        /// Any extra data required can be stored here.
        /// </summary>
        public required Dictionary<string, object> ExtraData { get; init; }
    }
}