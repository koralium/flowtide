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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class StreamEventValueContainer : IValueContainer<IStreamEvent>
    {
        internal List<IStreamEvent> _streamEvents;
        
        public int Count => _streamEvents.Count;

        public StreamEventValueContainer(IMemoryAllocator memoryAllocator)
        {
            _streamEvents = new List<IStreamEvent>();
        }

        public void AddRangeFrom(IValueContainer<IStreamEvent> container, int start, int count)
        {
            if (container is StreamEventValueContainer streamEventValueContainer)
            {
                _streamEvents.AddRange(streamEventValueContainer._streamEvents.GetRange(start, count));
                return;
            }
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            foreach(var e in _streamEvents)
            {
                if (e is IDisposable disposable)
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
            int size = 0;
            foreach (var e in _streamEvents)
            {
                if (e is StreamMessage<StreamEventBatch> streamEventBatchMessage)
                {
                    size += streamEventBatchMessage.Data.Data.EventBatchData.GetByteSize();
                    size += streamEventBatchMessage.Data.Data.Count * (sizeof(long) * 2); //Add for weight and iteration count
                }
                else
                {
                    size += 100; //Static size of 100 bytes for all other types
                }
            }
            return size;
        }

        public int GetByteSize(int start, int end)
        {
            int size = 0;

            for (int i = start; i < end; i++)
            {
                var e = _streamEvents[i];
                if (e is StreamMessage<StreamEventBatch> streamEventBatchMessage)
                {
                    size += streamEventBatchMessage.Data.Data.EventBatchData.GetByteSize();
                    size += streamEventBatchMessage.Data.Data.Count * (sizeof(long) * 2); //Add for weight and iteration count
                }
                else
                {
                    size += 100; //Static size of 100 bytes for all other types
                }
            }

            return size;
        }

        public ref IStreamEvent GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, IStreamEvent value)
        {
            _streamEvents.Insert(index, value);
        }

        public void RemoveAt(int index)
        {
            _streamEvents.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _streamEvents.RemoveRange(start, count);
        }

        public void Update(int index, IStreamEvent value)
        {
            _streamEvents[index] = value;
        }
    }
}
