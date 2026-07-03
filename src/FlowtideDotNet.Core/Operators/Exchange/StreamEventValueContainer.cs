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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Value container for stream events.
    ///
    /// The container owns a rent on every value it stores, taken when a value is added and
    /// returned when the value is removed or the container is disposed. Events are created
    /// with a rent count of zero and every holder takes its own rent, so a value handed out
    /// of the container stays alive through its receivers rent even when the page that
    /// contained it is disposed.
    /// </summary>
    internal class StreamEventValueContainer : IValueContainer<IStreamEvent>
    {
        internal List<IStreamEvent> _streamEvents;
        // Incrementally updated byte size, kept up to date on all mutations so GetByteSize
        // does not need to iterate all events each call.
        private int _byteSize = 0;

        public int Count => _streamEvents.Count;

        public StreamEventValueContainer(IMemoryAllocator memoryAllocator)
        {
            _streamEvents = new List<IStreamEvent>();
        }

        private static int GetEventByteSize(IStreamEvent streamEvent)
        {
            if (streamEvent is StreamMessage<StreamEventBatch> streamEventBatchMessage)
            {
                return streamEventBatchMessage.Data.Data.EventBatchData.GetByteSize() +
                    streamEventBatchMessage.Data.Data.Count * (sizeof(long) * 2); //Add for weight and iteration count
            }
            return 100; //Static size of 100 bytes for all other types
        }

        private static void RentEvent(IStreamEvent value)
        {
            if (value is StreamMessage<StreamEventBatch> streamMessage)
            {
                streamMessage.Data.Rent(1);
            }
            else if (value is IRentable rentable)
            {
                rentable.Rent(1);
            }
        }

        private static void ReturnEvent(IStreamEvent value)
        {
            if (value is StreamMessage<StreamEventBatch> streamMessage)
            {
                streamMessage.Data.Return();
            }
            else if (value is IRentable rentable)
            {
                rentable.Return();
            }
        }

        public void AddRangeFrom(IValueContainer<IStreamEvent> container, int start, int count)
        {
            if (container is StreamEventValueContainer streamEventValueContainer)
            {
                for (int i = start; i < start + count; i++)
                {
                    var value = streamEventValueContainer._streamEvents[i];
                    _byteSize += GetEventByteSize(value);
                    RentEvent(value);
                }
                _streamEvents.AddRange(streamEventValueContainer._streamEvents.GetRange(start, count));
                return;
            }
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            foreach(var e in _streamEvents)
            {
                ReturnEvent(e);
            }
        }

        public IStreamEvent Get(int index)
        {
            return _streamEvents[index];
        }

        public int GetByteSize()
        {
            return _byteSize;
        }

        public int GetByteSize(int start, int end)
        {
            int size = 0;

            for (int i = start; i < end; i++)
            {
                size += GetEventByteSize(_streamEvents[i]);
            }

            return size;
        }

        public ref IStreamEvent GetRef(int index)
        {
            throw new NotImplementedException();
        }

        internal void Add(IStreamEvent value)
        {
            _byteSize += GetEventByteSize(value);
            RentEvent(value);
            _streamEvents.Add(value);
        }

        public void Insert(int index, IStreamEvent value)
        {
            _byteSize += GetEventByteSize(value);
            RentEvent(value);
            _streamEvents.Insert(index, value);
        }

        public void RemoveAt(int index)
        {
            var value = _streamEvents[index];
            _byteSize -= GetEventByteSize(value);
            _streamEvents.RemoveAt(index);
            ReturnEvent(value);
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = start; i < start + count; i++)
            {
                var value = _streamEvents[i];
                _byteSize -= GetEventByteSize(value);
                ReturnEvent(value);
            }
            _streamEvents.RemoveRange(start, count);
        }

        public void Update(int index, IStreamEvent value)
        {
            var oldValue = _streamEvents[index];
            _byteSize -= GetEventByteSize(oldValue);
            _byteSize += GetEventByteSize(value);
            // Rent the new value before returning the old, they can be the same event.
            RentEvent(value);
            _streamEvents[index] = value;
            ReturnEvent(oldValue);
        }

        public void InsertFrom(IStreamEvent[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions)
        {
            throw new NotImplementedException();
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            throw new NotImplementedException();
        }
    }
}
