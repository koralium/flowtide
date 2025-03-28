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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Queue.Internal
{
    internal class FlowtideQueueMetadata : IStorageMetadata
    {
        public long Left { get; set; }

        public long Right { get; set; }

        public bool Updated { get; set; }

        /// <summary>
        /// Counter that checks which index that has been dequeued in the most left node.
        /// This is reset to 0 when the Left index is changed.
        /// </summary>
        public int DequeueIndex { get; set; }

        /// <summary>
        /// Insert index in the most right node, this can differ than the actual count in the node
        /// if a user uses the queue as a LIFO queue.
        /// </summary>
        public int InsertIndex { get; set; }

        public long QueueSize { get; set; }
    }
}
