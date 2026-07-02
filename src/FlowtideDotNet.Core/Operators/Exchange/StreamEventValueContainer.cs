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

        public void AddRangeFrom(IValueContainer<IStreamEvent> container, int start, int count)
        {
            if (container is StreamEventValueContainer streamEventValueContainer)
            {
                for (int i = start; i < start + count; i++)
                {
                    _byteSize += GetEventByteSize(streamEventValueContainer._streamEvents[i]);
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
                if (e is StreamMessage<StreamEventBatch> streamMessage)
                {
                    streamMessage.Data.Return();
                }
                else if (e is IRentable rentable)
                {
                    rentable.Return();
                }
                else if (e is IDisposable disposable)
                {
                    disposable.Dispose();
                }
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
            _streamEvents.Add(value);
        }

        public void Insert(int index, IStreamEvent value)
        {
            _byteSize += GetEventByteSize(value);
            _streamEvents.Insert(index, value);
        }

        public void RemoveAt(int index)
        {
            _byteSize -= GetEventByteSize(_streamEvents[index]);
            _streamEvents.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = start; i < start + count; i++)
            {
                _byteSize -= GetEventByteSize(_streamEvents[i]);
            }
            _streamEvents.RemoveRange(start, count);
        }

        public void Update(int index, IStreamEvent value)
        {
            _byteSize -= GetEventByteSize(_streamEvents[index]);
            _byteSize += GetEventByteSize(value);
            _streamEvents[index] = value;
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
