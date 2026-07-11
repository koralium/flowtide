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

using FlowtideDotNet.Core.Operators.Exchange;
using Orleans.Serialization.Buffers;

namespace FlowtideDotNet.Cluster.Orleans.Messages
{
    /// <summary>
    /// Fetched substream events, serialized only when they actually cross a silo boundary.
    ///
    /// The serving grain hands the live events over by reference; between grains on the same
    /// silo the payload instance passes through Orleans uncopied (identity copier) and the
    /// receiver consumes the live events with zero copies, taking over the receiver claim
    /// exactly like a local fetch. Only when Orleans serializes the message does the codec
    /// write the events in the substream wire format, releasing the claims on the sending
    /// side; the receiving silo captures the raw payload without materializing it, and the
    /// communication handler deserializes lazily with the consuming operator's allocator.
    ///
    /// Ownership is one-shot: exactly one consumer takes the live events (the same-silo
    /// receiver or the serializing codec). A payload is bound to one response.
    /// </summary>
    public sealed class SubstreamEventsPayload
    {
        private IReadOnlyList<SubstreamEventData>? _liveEvents;
        private PooledBuffer _serializedEvents;
        private readonly bool _isSerialized;

        private SubstreamEventsPayload(IReadOnlyList<SubstreamEventData> liveEvents)
        {
            _liveEvents = liveEvents;
        }

        internal SubstreamEventsPayload(PooledBuffer serializedEvents)
        {
            _serializedEvents = serializedEvents;
            _isSerialized = true;
        }

        /// <summary>
        /// Wraps live events for a fetch response. The payload takes over the receiver
        /// claims; the caller must not touch the events afterwards.
        /// </summary>
        public static SubstreamEventsPayload FromLive(IReadOnlyList<SubstreamEventData> events)
        {
            return new SubstreamEventsPayload(events);
        }

        /// <summary>
        /// True when the payload crossed a silo boundary and holds the wire format instead
        /// of live events.
        /// </summary>
        public bool IsSerialized => _isSerialized;

        /// <summary>
        /// Takes the live events and their receiver claims. One-shot: the same-silo receiver
        /// and the serializing codec race at most conceptually, never actually, but the
        /// exchange keeps a bug from turning into a double release of the claims.
        /// </summary>
        public bool TryTakeLive(out IReadOnlyList<SubstreamEventData> events)
        {
            var taken = Interlocked.Exchange(ref _liveEvents, null);
            if (taken != null)
            {
                events = taken;
                return true;
            }
            events = Array.Empty<SubstreamEventData>();
            return false;
        }

        /// <summary>
        /// Takes the serialized wire payload after a silo crossing. The caller owns and
        /// disposes the buffer.
        /// </summary>
        internal PooledBuffer TakeSerialized()
        {
            var buffer = _serializedEvents;
            _serializedEvents = default;
            return buffer;
        }
    }
}
