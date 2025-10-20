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

using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    internal class BufferSegment : ReadOnlySequenceSegment<byte>, IDisposable
    {
        private BufferSegment? _next;
        private int _end;
        private IMemoryOwner<byte>? _owner;
        private int _rentCounter;

        public Memory<byte> AvailableMemory { get; private set; }

        public int Length => End;

        public int End
        {
            get => _end;
            set
            {
                Debug.Assert(value <= AvailableMemory.Length);

                _end = value;
                Memory = AvailableMemory.Slice(0, value);
            }
        }

        public BufferSegment(IMemoryOwner<byte> memory)
        {
            _owner = memory;
            this.Memory = _owner.Memory;
            AvailableMemory = _owner.Memory;
            _end = _owner.Memory.Length;
        }

        public void SetNext(BufferSegment segment)
        {
            Debug.Assert(segment != null);
            Debug.Assert(Next == null);

            _next = segment;

            segment = this;

            while (segment.Next != null)
            {
                Debug.Assert(segment._next != null);
                segment._next.RunningIndex = segment.RunningIndex + segment.Length;
                segment = segment._next;
            }
        }

        public bool TryRent()
        {
            var localRentCount = Thread.VolatileRead(ref _rentCounter);
            if (localRentCount == 0)
            {
                return false;
            }
            int incrementedValue;
            // Run a loop to try and add the new value, if compare exchange fails, add to the new returned value + 1, if 0 return false
            do
            {
                incrementedValue = localRentCount;
                localRentCount = Interlocked.CompareExchange(ref _rentCounter, incrementedValue + 1, localRentCount);
                if (localRentCount == 0)
                {
                    return false;
                }
            } while (localRentCount != incrementedValue);

            return true;
        }

        public void Return()
        {
            var result = Interlocked.Decrement(ref _rentCounter);

            if (result <= 0)
            {
                Dispose();
            }
        }

        public void Dispose()
        {
            if (_owner != null)
            {
                _owner.Dispose();
                _owner = null;
            }
        }
    }
}
