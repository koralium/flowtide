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

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    /// <summary>
    /// Contains version information of a checkpoint and also if that version is a snapshot.
    /// </summary>
    public class CheckpointVersion
    {
        /// <summary>
        /// The version of a checkpoint
        /// </summary>
        public long Version { get; }

        /// <summary>
        /// Indicates whether the checkpoint is a snapshot checkpoint or not. Snapshot checkpoints are checkpoints that contain the full 
        /// state of the system at a given version, while non-snapshot checkpoints may only contain incremental changes since the last snapshot checkpoint. 
        /// This information can be used to optimize checkpoint loading and recovery processes, as loading from a snapshot checkpoint can be faster than 
        /// applying a series of incremental checkpoints.
        /// </summary>
        public bool IsSnapshot { get; }

        public CheckpointVersion(long version, bool isSnapshot)
        {
            Version = version;
            IsSnapshot = isSnapshot;
        }
    }
}
