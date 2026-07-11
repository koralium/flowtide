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
    /// Single place for rent/return type dispatch of exchange stream events. Every holder takes a
    /// claim with <see cref="Rent"/> and releases it with <see cref="Return"/> (for a stream message
    /// the claim belongs to the batch data), so a new event type is only handled here.
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
