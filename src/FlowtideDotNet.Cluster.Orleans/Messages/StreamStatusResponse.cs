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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;

namespace FlowtideDotNet.Cluster.Orleans.Messages
{
    /// <summary>
    /// Status of one substream, reported by its substream grain.
    /// </summary>
    [GenerateSerializer]
    public class SubstreamStatus
    {
        [Id(0)]
        public string? SubstreamName { get; set; }

        /// <summary>
        /// True when the substream grain has a persisted start record, so it is expected
        /// to be running and its keep alive watchdog restarts it after failures.
        /// </summary>
        [Id(1)]
        public bool IsStarted { get; set; }

        /// <summary>
        /// State of the stream state machine, null when no stream instance is running in
        /// the grain, for example because the last start attempt failed.
        /// </summary>
        [Id(2)]
        public StreamStateValue? State { get; set; }

        /// <summary>
        /// Health of the stream, null when no stream instance is running in the grain.
        /// </summary>
        [Id(3)]
        public FlowtideHealth? Health { get; set; }

        /// <summary>
        /// Most recent failure on the current grain activation, null when none. Failures are retried
        /// in the background; a running state next to a non-null value means the stream recovered.
        /// </summary>
        [Id(4)]
        public string? LastFailure { get; set; }

        /// <summary>
        /// Set when the substream grain could not be reached at all, the other fields are
        /// then unknown.
        /// </summary>
        [Id(5)]
        public string? Error { get; set; }

        /// <summary>
        /// Identifier of the grain activation that served this status. Changes when the grain
        /// is reactivated, for example after a silo failure or an activation migration.
        /// </summary>
        [Id(6)]
        public string? ActivationId { get; set; }
    }

    /// <summary>
    /// Status of a stream and all substreams it started.
    /// </summary>
    [GenerateSerializer]
    public class StreamStatusResponse
    {
        /// <summary>
        /// True when the stream has been started and not stopped since.
        /// </summary>
        [Id(0)]
        public bool IsStarted { get; set; }

        /// <summary>
        /// Status per started substream, empty when the stream is not started.
        /// </summary>
        [Id(1)]
        public List<SubstreamStatus> Substreams { get; set; } = new List<SubstreamStatus>();
    }
}
