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

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Represents the overall health status of the Flowtide stream.
    /// </summary>
    /// <remarks>
    /// This enum is used to report the operational health of the stream, 
    /// allowing external monitoring systems or health checks to determine if the stream 
    /// is functioning normally, experiencing problems, or effectively down.
    /// </remarks>
    public enum FlowtideHealth
    {
        /// <summary>
        /// Indicates that the stream is fully operational and processing data as expected without issues.
        /// </summary>
        Healthy,

        /// <summary>
        /// Indicates that the stream is running but experiencing degraded performance or minor transient errors.
        /// Processing may be slower, but the system has not entirely failed.
        /// </summary>
        Degraded,

        /// <summary>
        /// Indicates that the stream has encountered severe errors or critical failures and is no longer able to process data correctly.
        /// </summary>
        Unhealthy
    }
}
