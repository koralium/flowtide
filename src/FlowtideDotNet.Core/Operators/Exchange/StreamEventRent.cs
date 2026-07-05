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

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// The single owner of the rent and return type dispatch for stream events in the
    /// exchange. Batches are reference counted: every holder takes a claim with
    /// <see cref="Rent"/> and releases it with <see cref="Return"/>, an imbalance either
    /// frees memory that is still in use or leaks it. For a
    /// <see cref="StreamMessage{T}"/> carrying a <see cref="StreamEventBatch"/> the claim
    /// belongs to the batch data, not the message wrapper. Every place that holds or
    /// releases events must go through this class, a second copy of this dispatch drifts
    /// and a new event type then leaks or double frees in the copies that were not updated.
    /// </summary>
    internal static class StreamEventRent
    {
        /// <summary>
        /// Takes one reference claim on the event.
        /// </summary>
        public static void Rent(IStreamEvent streamEvent)
        {
            if (streamEvent is StreamMessage<StreamEventBatch> streamMessage)
            {
                if (streamMessage.Data is IRentable rentableData)
                {
                    rentableData.Rent(1);
                }
            }
            else if (streamEvent is IRentable rentable)
            {
                rentable.Rent(1);
            }
        }

        /// <summary>
        /// Releases one reference claim on the event.
        /// </summary>
        public static void Return(IStreamEvent streamEvent)
        {
            if (streamEvent is StreamMessage<StreamEventBatch> streamMessage)
            {
                if (streamMessage.Data is IRentable rentableData)
                {
                    rentableData.Return();
                }
            }
            else if (streamEvent is IRentable rentable)
            {
                rentable.Return();
            }
        }

        /// <summary>
        /// Releases an event that leaves the system, for example an undeliverable fetched
        /// event during a recovery. Rentable events release their claim, other events that
        /// own resources are disposed.
        /// </summary>
        public static void Dispose(IStreamEvent streamEvent)
        {
            if (streamEvent is StreamMessage<StreamEventBatch> streamMessage)
            {
                if (streamMessage.Data is IRentable rentableData)
                {
                    rentableData.Return();
                }
            }
            else if (streamEvent is IRentable rentable)
            {
                rentable.Return();
            }
            else if (streamEvent is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }
}
