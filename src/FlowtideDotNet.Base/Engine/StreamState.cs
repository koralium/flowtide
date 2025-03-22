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

namespace FlowtideDotNet.Base.Engine
{
    public class StreamState
    {
        public static readonly StreamState NullState = new StreamState(-1, 0, string.Empty);

        /// <summary>
        /// The stream time this checkpoint refers to.
        /// </summary>
        public long Time { get; }

        public long StrreamVersion { get; }

        public string StreamHash { get; }

        public StreamState(long time, long strreamVersion, string streamHash)
        {
            Time = time;
            StrreamVersion = strreamVersion;
            StreamHash = streamHash;
        }
    }
}
