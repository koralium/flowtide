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
        /// The fetched events serialized with the substream event wire format, an opaque
        /// buffer so the events can cross silo boundaries without Orleans knowing about the
        /// event types. Data batches use the same columnar encoding as the exchange queues
        /// use when pages spill to disk.
        ///
        /// A <see cref="PooledBuffer"/> so no byte arrays are allocated for the payload, the
        /// events are written into pooled segments which the Orleans codec sends directly.
        /// Ownership: the consumer of the response disposes the buffer, on a cross silo call
        /// the codec consumes it when the response is serialized and the receiving side gets
        /// its own pooled copy, on a same silo call the receiver gets this instance since the
        /// response is immutable. The producing grain must not touch it after returning.
        /// </summary>
        [Id(0)]
        public PooledBuffer Events { get; set; }

        /// <summary>
        /// True when the serving grain does not know the requestors announced fetch epoch,
        /// for example after the serving grain was reactivated on another silo and lost its
        /// per activation epoch table. The fetching side must not treat this like an empty
        /// poll: refused fetches never return data, and a persistently refused fetcher must
        /// fail and recover so the initialize handshake re-announces its epoch.
        /// </summary>
        [Id(1)]
        public bool RequestorUnknown { get; set; }
    }
}
