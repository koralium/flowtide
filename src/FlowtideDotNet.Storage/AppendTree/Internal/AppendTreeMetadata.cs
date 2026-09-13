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

namespace FlowtideDotNet.Storage.AppendTree.Internal
{
    internal class AppendTreeMetadata : IStorageMetadata
    {
        private long _root;
        private long _right;

        public int BucketLength { get; set; }

        public long Root
        {
            get => _root;
            set
            {
                if (_root != value)
                {
                    _root = value;
                    Updated = true;
                }
            }
        }

        /// <summary>
        /// Contains the id of the most left page.
        /// This is used to start an iterator.
        /// </summary>
        public long Left { get; set; }

        public long Right
        {
            get => _right;
            set
            {
                if (_right != value)
                {
                    _right = value;
                    Updated = true;
                }
            }
        }

        public int Depth { get; set; }

        [JsonIgnore]
        public bool Updated { get; set; }

        public AppendTreeMetadata()
        {
            Updated = true;
        }

        [JsonConstructor]
        public AppendTreeMetadata(int bucketLength, long root, long left, long right, int depth)
        {
            BucketLength = bucketLength;
            _root = root;
            Left = left;
            _right = right;
            Depth = depth;
            Updated = false;
        }
    }
}
