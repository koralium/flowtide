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

using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils
{
    internal class LogTransactionFile
    {
        public string FileName { get; }

        public bool IsCheckpoint { get; }

        public bool IsJson { get; }

        public long Version { get; }

        public IOEntry IOEntry { get; }

        public bool IsCompacted { get; }

        public LogTransactionFile(string fileName, bool isCheckpoint, bool isJson, long version, IOEntry ioEntry, bool isCompacted)
        {
            FileName = fileName;
            IsCheckpoint = isCheckpoint;
            IsJson = isJson;
            Version = version;
            IOEntry = ioEntry;
            IsCompacted = isCompacted;
        }

    }
}
