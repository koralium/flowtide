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
    /// Grain running one substream of a stream.
    ///
    /// Most methods on this interface are the internal protocol between the substream
    /// grains: fetching exchanged data, checkpoint notifications, recovery and the
    /// initialize handshake. Do not call them from application code, and do not start,
    /// stop or delete a single substream directly — a substream acting alone breaks the
    /// coordinated checkpointing of its peers. Use <see cref="IStreamGrain"/> to manage
    /// streams.
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
        /// Returns the status of this substream, including the failure of the last start
        /// attempt when the stream could not start.
        /// </summary>
        Task<SubstreamStatus> GetStatusAsync();

        /// <summary>
        /// Stops the substream and deletes its state, completing when the deletion has
        /// finished. All substreams of a stream should be deleted together through the
        /// stream grain, a substream whose peers keep running would be recovered against
        /// missing state.
        /// </summary>
        Task DeleteStreamAsync();
    }
}
