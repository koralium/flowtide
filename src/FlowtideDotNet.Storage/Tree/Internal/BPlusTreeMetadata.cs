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

using FlowtideDotNet.Storage.StateManager;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeMetadata : IStorageMetadata
    {
        public static BPlusTreeMetadata Create(int bucketLength, long root, long left, int pageSizeBytes, List<long> keyMetadataPages, List<long> valueMetadataPages)
        {
            var newMetadata = new BPlusTreeMetadata(bucketLength, root, left, pageSizeBytes, keyMetadataPages, valueMetadataPages);
            newMetadata.Updated = true;
            return newMetadata;
        }

        /// <summary>
        /// Constructor is used for serialization
        /// </summary>
        /// <param name="bucketLength"></param>
        /// <param name="root"></param>
        /// <param name="left"></param>
        /// <param name="pageSizeBytes"></param>
        [JsonConstructor]
        public BPlusTreeMetadata(int bucketLength, long root, long left, int pageSizeBytes, List<long> keyMetadataPages, List<long> valueMetadataPages)
        {
            BucketLength = bucketLength;
            Root = root;
            Left = left;
            PageSizeBytes = pageSizeBytes;
            Updated = false;
            KeyMetadataPages = keyMetadataPages;
            ValueMetadataPages = valueMetadataPages;
        }

        public int BucketLength { get; }
        public long Root { get; }

        /// <summary>
        /// Contains the id of the most left page.
        /// This is used to start an iterator.
        /// </summary>
        public long Left { get; }
        public int PageSizeBytes { get; }

        public List<long> KeyMetadataPages { get; }

        public List<long> ValueMetadataPages { get; }

        [JsonIgnore]
        public bool Updated { get; set; }

        public BPlusTreeMetadata UpdateRoot(long newRoot)
        {
            var newMetadata = new BPlusTreeMetadata(BucketLength, newRoot, Left, PageSizeBytes, KeyMetadataPages, ValueMetadataPages);
            newMetadata.Updated = true;
            return newMetadata;
        }
    }
}
