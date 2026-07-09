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

using FlowtideDotNet.Orleans.Messages;
using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Interfaces
{
    /// <summary>
    /// Grain running one substream. Most methods are the internal protocol between substream grains;
    /// don't call them from application code or manage a single substream directly, use
    /// <see cref="IStreamGrain"/>.
    /// </summary>
    public interface ISubStreamGrain : IGrainWithStringKey
    {
        Task StartStreamAsync(StartStreamMessage startStreamMessage);

        Task<GetEventsResponse> GetEventsAsync(GetEventsRequest request);

        Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request);

        Task FailAndRecoverAsync(FailAndRecoverRequest request);

        Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request);

        Task CheckpointDone(CheckpointDoneRequest request);

        /// <summary>
        /// Stops the substream gracefully. All substreams of a stream should be stopped
        /// together, for example through the stream grain, so the coordinated stop can drain
        /// the data exchanged between the substreams.
        /// </summary>
        Task StopStreamAsync();

        /// <summary>
        /// Returns the status of this substream, including the most recent stream failure,
        /// which is how a stream that cannot start reports why.
        /// </summary>
        Task<SubstreamStatus> GetStatusAsync();

        /// <summary>
        /// Stops the substream and deletes its state. Delete all substreams together through the
        /// stream grain, or a substream whose peers keep running would be recovered against missing state.
        /// </summary>
        Task DeleteStreamAsync();

        /// <summary>
        /// Requests the substream grain activation to migrate to another silo, for example to
        /// rebalance load. The activation deactivates once its current calls complete and is
        /// placed again by the runtime; the substream resumes on the new activation.
        /// </summary>
        Task MigrateAsync();
    }
}
