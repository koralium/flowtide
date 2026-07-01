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

namespace FlowtideDotNet.Storage
{
    /// <summary>
    /// Identifies the version of a running stream so that persisted state can be matched to the plan that produced it.
    /// It is supplied through the stream builder (<c>SetVersionInformation</c>) and flows into storage initialization,
    /// where providers use it to detect plan changes between restarts and to scope stored data to a particular version.
    /// </summary>
    public class StreamVersionInformation
    {
        /// <summary>
        /// Creates version information for a stream.
        /// </summary>
        /// <param name="hash">A content hash of the stream plan, used to detect structural changes.</param>
        /// <param name="version">A human-readable version label for the stream configuration.</param>
        public StreamVersionInformation(string hash, string version)
        {
            Hash = hash;
            Version = version;
        }

        /// <summary>
        /// A content hash of the stream plan. On startup it is compared against the hash stored in persisted state;
        /// a mismatch indicates the stream topology has changed and is used to guard against incompatible state restores.
        /// </summary>
        public string Hash { get; }

        /// <summary>
        /// A human-readable version label associated with the stream configuration. Storage providers can use it to
        /// scope stored data to a particular stream version.
        /// </summary>
        public string Version { get; }
    }
}
