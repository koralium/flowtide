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

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal sealed class StreamInfo
    {
        private StreamMetadata _metadata;

        internal StreamInfo(string name, int streamKey, int currentVersion, int lastSucessfulVersion)
        {
            _metadata = new(name, streamKey, currentVersion, lastSucessfulVersion);
        }

        internal StreamMetadata Metadata => _metadata;

        internal void IncrementVersion()
        {
            var nextVersion = _metadata.CurrentVersion + 1;
            _metadata = new(_metadata.Name, _metadata.StreamKey, nextVersion, _metadata.CurrentVersion);
        }

        internal void Reset()
        {
            _metadata = new(_metadata.Name, _metadata.StreamKey, 0, 0);
        }

        internal record StreamMetadata(string Name, int StreamKey, int CurrentVersion, int LastSucessfulVersion);
    }
}
