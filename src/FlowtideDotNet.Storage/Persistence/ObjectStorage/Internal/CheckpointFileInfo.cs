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

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class CheckpointFileInfo
    {
        private readonly string filePath;
        private readonly long version;
        private bool isCheckpoint;
        private bool isSnapshot;

        public CheckpointFileInfo(string filePath)
        {
            this.filePath = filePath;

            var lastSlashIndex = filePath.LastIndexOf('/');
            var lastBackslashIndex = filePath.LastIndexOf('\\');

            lastSlashIndex = Math.Max(lastSlashIndex, lastBackslashIndex);

            var dotIndex = filePath.IndexOf('.', lastSlashIndex);

            if (dotIndex >= 0)
            {
                var versionString = filePath.Substring(lastSlashIndex + 1, dotIndex - lastSlashIndex - 1);
                long.TryParse(versionString, out version);
            }

            var afterDotString = filePath.Substring(dotIndex + 1);

            if (afterDotString == "checkpoint")
            {
                isCheckpoint = true;
            }
            else if (afterDotString == "snapshot.checkpoint")
            {
                isCheckpoint = true;
                isSnapshot = true;
            }
        }

        public bool IsCheckpoint => isCheckpoint;

        public long Version => version;

        public string FilePath => filePath;

        public bool IsSnapshot => isSnapshot;
    }
}
