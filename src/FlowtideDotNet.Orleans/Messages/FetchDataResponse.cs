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

using Orleans.Serialization.Buffers;

namespace FlowtideDotNet.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class FetchDataResponse
    {
        public FetchDataResponse(PooledBuffer events)
        {
            Events = events;
        }

        /// <summary>
        /// The fetched events in the substream event wire format, an opaque <see cref="PooledBuffer"/>
        /// so they cross silo boundaries without Orleans knowing the event types and without allocating
        /// byte arrays. Ownership: the response consumer disposes it (cross-silo the codec consumes it
        /// and the receiver gets its own pooled copy, same-silo the receiver gets this instance); the
        /// producing grain must not touch it after returning.
        /// </summary>
        [Id(0)]
        public PooledBuffer Events { get; set; }

        /// <summary>
        /// True when the requestors fetch epoch is not the one announced to the serving grain (the
        /// grain lost its epoch table after a reactivation, or another instance announced a different
        /// epoch). The fetcher must not treat this as an empty poll: a persistently refused fetcher
        /// fails and recovers so the initialize handshake re-announces its epoch.
        /// </summary>
        [Id(1)]
        public bool RequestorUnknown { get; set; }
    }
}
