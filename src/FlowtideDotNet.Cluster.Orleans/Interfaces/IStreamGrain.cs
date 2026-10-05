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

using FlowtideDotNet.Cluster.Orleans.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Cluster.Orleans.Interfaces
{
    public interface IStreamGrain : IGrainWithStringKey
    {
        /// <summary>
        /// Starts all substreams. Starting an already started stream with the same SQL and substream
        /// count is a no-op; a different plan throws (stop it first). Returns once the grains accept
        /// the start; the streams start in the background, poll <see cref="GetStatusAsync"/> for health.
        /// </summary>
        Task StartStreamAsync(StartStreamRequest request);

        /// <summary>
        /// Returns the status of the stream and each substream it started, including start
        /// failures that happened in the background after <see cref="StartStreamAsync"/>
        /// returned.
        /// </summary>
        Task<StreamStatusResponse> GetStatusAsync();

        /// <summary>
        /// Stops all substreams and deletes their state, completing when the deletion has
        /// finished. Only substreams recorded as started are reached: a stopped stream has
        /// no recorded substreams, to delete its state start it again first and then delete.
        /// </summary>
        Task DeleteStreamAsync();

        /// <summary>
        /// Stops all substreams together, so the coordinated stop drains the data exchanged between
        /// them and everything sent before the stop is part of the final checkpoints. The grain
        /// persists which substreams it started, so the caller supplies nothing.
        /// </summary>
        Task StopStreamAsync();
    }
}
