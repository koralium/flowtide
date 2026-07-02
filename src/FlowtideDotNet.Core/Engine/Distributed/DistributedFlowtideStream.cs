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

namespace FlowtideDotNet.Core.Engine.Distributed
{
    /// <summary>
    /// A distributed stream where all substreams run in the same process.
    /// Built with <see cref="DistributedStreamBuilder"/>.
    /// </summary>
    public sealed class DistributedFlowtideStream : IAsyncDisposable
    {
        private readonly IReadOnlyDictionary<string, Base.Engine.DataflowStream> _substreams;

        internal DistributedFlowtideStream(string streamName, IReadOnlyDictionary<string, Base.Engine.DataflowStream> substreams)
        {
            StreamName = streamName;
            _substreams = substreams;
        }

        public string StreamName { get; }

        /// <summary>
        /// The substreams by substream name.
        /// </summary>
        public IReadOnlyDictionary<string, Base.Engine.DataflowStream> Substreams => _substreams;

        /// <summary>
        /// Starts all substreams.
        /// </summary>
        public Task StartAsync()
        {
            return Task.WhenAll(_substreams.Values.Select(x => x.StartAsync()));
        }

        /// <summary>
        /// Stops all substreams.
        /// The substreams are stopped in parallel since a final checkpoint requires
        /// communication between the substreams.
        /// </summary>
        public Task StopAsync()
        {
            return Task.WhenAll(_substreams.Values.Select(x => x.StopAsync()));
        }

        /// <summary>
        /// Deletes the state of all substreams.
        /// </summary>
        public Task DeleteAsync()
        {
            return Task.WhenAll(_substreams.Values.Select(x => x.DeleteAsync()));
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var substream in _substreams.Values)
            {
                await substream.DisposeAsync();
            }
        }
    }
}
