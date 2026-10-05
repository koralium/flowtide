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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    /// <summary>
    /// Disposing is the rule, but a dropped column must still release its native memory
    /// </summary>
    public class ColumnFinalizerReclaimTests
    {
        // Counts only this test's allocations, a global counter would drift under parallel tests
        private sealed class TrackingAllocator : IMemoryAllocator
        {
            private int _active;
            private long _outstandingBytes;

            public int Active => Volatile.Read(ref _active);

            public long OutstandingBytes => Interlocked.Read(ref _outstandingBytes);

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                return Track(GlobalMemoryManager.Instance.Allocate(size, alignment));
            }

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                var owner = (Owner)memory;
                return Track(GlobalMemoryManager.Instance.Realloc(owner.Release(), size, alignment));
            }

            private IMemoryOwner<byte> Track(IMemoryOwner<byte> inner)
            {
                Interlocked.Increment(ref _active);
                return new Owner(inner, this);
            }

            // FlowtideMemory is allocated by the default interface methods, which report every
            // block here, so the outstanding total is what tracks the native path
            public void RegisterAllocationToMetrics(int size) => Interlocked.Add(ref _outstandingBytes, size);

            public void RegisterFreeToMetrics(int size) => Interlocked.Add(ref _outstandingBytes, -size);

            private void Released() => Interlocked.Decrement(ref _active);

            private sealed class Owner : IMemoryOwner<byte>
            {
                private readonly IMemoryOwner<byte> _inner;
                private readonly TrackingAllocator _parent;
                private bool _released;

                public Owner(IMemoryOwner<byte> inner, TrackingAllocator parent)
                {
                    _inner = inner;
                    _parent = parent;
                }

                public Memory<byte> Memory => _inner.Memory;

                // Hands the inner owner to Realloc, which frees it, so this wrapper stops counting
                public IMemoryOwner<byte> Release()
                {
                    if (!_released)
                    {
                        _released = true;
                        _parent.Released();
                    }
                    return _inner;
                }

                public void Dispose()
                {
                    if (!_released)
                    {
                        _released = true;
                        _parent.Released();
                        _inner.Dispose();
                    }
                }
            }
        }

        // Built out of line so no stack slot keeps the column alive during the collect
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void BuildAndDrop(IMemoryAllocator allocator, Action<Column> fill)
        {
            var column = new Column(allocator);
            fill(column);
        }

        private static void AssertReleasedWithoutDispose(Action<Column> fill)
        {
            var allocator = new TrackingAllocator();

            BuildAndDrop(allocator, fill);
            Assert.True(allocator.Active > 0 || allocator.OutstandingBytes > 0, "The column never allocated, the test proves nothing");

            for (int i = 0; i < 3; i++)
            {
                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
            }

            Assert.Equal(0, allocator.Active);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        [Fact]
        public void DroppedStringColumnReleasesNativeMemory()
        {
            AssertReleasedWithoutDispose(column =>
            {
                for (int i = 0; i < 500; i++)
                {
                    column.Add(new StringValue($"value {i}"));
                }
            });
        }

        [Fact]
        public void DroppedBinaryColumnReleasesNativeMemory()
        {
            AssertReleasedWithoutDispose(column =>
            {
                for (int i = 0; i < 500; i++)
                {
                    column.Add(new BinaryValue(new byte[64]));
                }
            });
        }

        [Fact]
        public void DroppedListColumnReleasesNativeMemory()
        {
            AssertReleasedWithoutDispose(column =>
            {
                for (int i = 0; i < 200; i++)
                {
                    column.Add(new ListValue(new Int64Value(i), new Int64Value(i + 1)));
                }
            });
        }

        [Fact]
        public void DroppedIntColumnReleasesNativeMemory()
        {
            AssertReleasedWithoutDispose(column =>
            {
                for (int i = 0; i < 500; i++)
                {
                    column.Add(new Int64Value(i));
                }
            });
        }
    }
}
