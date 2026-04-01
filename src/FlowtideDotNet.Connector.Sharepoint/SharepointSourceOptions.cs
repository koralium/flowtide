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

namespace FlowtideDotNet.Connector.Sharepoint
{
    public class SharepointSourceOptions : SharepointOptions
    {
        /// <summary>
        /// Gets or sets the interval between consecutive delta load operations. Default 10 seconds.
        /// </summary>
        /// <remarks>Adjust this value to control how frequently incremental updates are performed.
        /// Setting a shorter interval may increase system responsiveness but causes more API calls to Sharepoint.</remarks>
        public TimeSpan DeltaLoadInterval { get; set; } = TimeSpan.FromSeconds(10);
    }
}
