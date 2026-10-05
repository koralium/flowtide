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

namespace FlowtideDotNet.Cluster.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class FetchDataResponse
    {
        public FetchDataResponse(SubstreamEventsPayload? events)
        {
            Events = events;
        }

        /// <summary>
        /// The fetched events. Same silo the payload passes by reference and the receiver
        /// consumes the live events with zero copies; only when the response crosses a silo
        /// boundary does the payload codec serialize them in the substream wire format, see
        /// <see cref="SubstreamEventsPayload"/>. Ownership moves with the response, the
        /// producing grain must not touch the events after returning.
        /// </summary>
        [Id(0)]
        public SubstreamEventsPayload? Events { get; set; }

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
