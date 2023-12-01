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

using FASTER.core;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.FileCache.Internal;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Debug;
using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests
{
    internal class WriteFailureDevice : IDevice
    {
        public uint SectorSize => 16;

        public string FileName => "failure";

        public long Capacity => long.MaxValue;

        public long SegmentSize => 4096 * 16;

        public int StartSegment => 0;

        public int EndSegment => 0;

        public int ThrottleLimit { get; set; }

        public void Dispose()
        {
            
        }

        public long GetFileSize(int segment)
        {
            return 0;
        }

        public void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
        }

        public void ReadAsync(int segmentId, ulong sourceAddress, nint destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            callback(9, 0, null);
        }

        public void ReadAsync(ulong alignedSourceAddress, nint alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
        {
            callback(9, 0, null);
        }

        public void RemoveSegment(int segment)
        {
            
        }

        public void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
        }

        public void Reset()
        {
            
        }

        public bool Throttle()
        {
            return false;
        }

        public void TruncateUntilAddress(long toAddress)
        {
            
        }

        public void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
        {
        }

        public void TruncateUntilSegment(int toSegment)
        {
        }

        public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
        {
        }

        public bool TryComplete()
        {
            return true;
        }

        public void WriteAsync(nint sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            callback(9, 0, null);
        }

        public void WriteAsync(nint alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            callback(9, 0, context);
        }
    }

    internal class ReadFailureDevice : IDevice
    {
        private readonly IDevice device;
        public bool readFailure = false;

        public ReadFailureDevice(IDevice device)
        {
            this.device = device;
        }

        public uint SectorSize => device.SectorSize;

        public string FileName => device.FileName;

        public long Capacity => device.Capacity;

        public long SegmentSize => device.SegmentSize;

        public int StartSegment => device.StartSegment;

        public int EndSegment => device.EndSegment;

        public int ThrottleLimit { get => device.ThrottleLimit; set { device.ThrottleLimit = value; } }

        public void Dispose()
        {
            device.Dispose();
        }

        public long GetFileSize(int segment)
        {
            return device.GetFileSize(segment);
        }

        public void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            device.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        public void ReadAsync(int segmentId, ulong sourceAddress, nint destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            device.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public void ReadAsync(ulong alignedSourceAddress, nint alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
        {
            if (readFailure)
            {
                callback(9, 0, context);
                return;
            }
            device.ReadAsync(alignedSourceAddress, alignedDestinationAddress, aligned_read_length, callback, context);
        }

        public void RemoveSegment(int segment)
        {
            device.RemoveSegment(segment);
        }

        public void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            device.RemoveSegmentAsync(segment, callback, result);
        }

        public void Reset()
        {
            device.Reset();
        }

        public bool Throttle()
        {
            return device.Throttle();
        }

        public void TruncateUntilAddress(long toAddress)
        {
            device.TruncateUntilAddress(toAddress);
        }

        public void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
        {
            device.TruncateUntilAddressAsync(toAddress, callback, result);
        }

        public void TruncateUntilSegment(int toSegment)
        {
            device.TruncateUntilSegment(toSegment);
        }

        public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
        {
            device.TruncateUntilSegmentAsync(toSegment, callback, result);
        }

        public bool TryComplete()
        {
            return device.TryComplete();
        }

        public void WriteAsync(nint sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            device.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        public void WriteAsync(nint alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            device.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, callback, context);
        }
    }


    public class DeviceFailureTests
    {
        /// <summary>
        /// This tests that the solution waits maximum 10 seconds before throwing an error during writes.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestWriteFailure()
        {
            var nullFactory = new NullLoggerFactory();
            var manager = new StateManagerSync<object>(() =>
            {
                return new StateManagerOptions()
                {
                    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
                    {
                        LogDevice = new WriteFailureDevice(),
                        MemorySize = 128,
                        PageSize = 128,
                        
                    }),
                };
            }, nullFactory.CreateLogger("logger"));
            await manager.InitializeAsync();
            var client = manager.GetOrCreateClient("test");
            var tree = await client.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });

            await tree.Upsert(3, "hello");
            await tree.Upsert(4, "hello1");
            await tree.Upsert(5, "hello1");
            await tree.Upsert(6, "hello1");
            await tree.Upsert(7, "hello1");
            await tree.Upsert(8, "hello1");
            await tree.Upsert(9, "hello1");
            await tree.Upsert(10, "hello1");
            await tree.Upsert(11, "hello1");
            await tree.Upsert(12, "hello1");
            await tree.Upsert(13, "hello1");
            await tree.Upsert(14, "hello1");
            await tree.Upsert(15, "hello1");
            await tree.Upsert(16, "hello1");
            await tree.Upsert(17, "hello1");
            await tree.Upsert(18, "hello1");
            await tree.Upsert(19, "hello1");

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await tree.Commit();
            });
        }

        [Fact]
        public async Task TestReadFailure()
        {
            var logger = new LoggerFactory(new List<ILoggerProvider>() { new DebugLoggerProvider() }).CreateLogger("test");
            var device = new ReadFailureDevice(Devices.CreateLogDevice("test", deleteOnClose: true));
            var manager = new StateManagerSync<object>(() =>
            {
                return new StateManagerOptions()
                {
                    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
                    {
                        LogDevice = device,
                        MemorySize = 512,
                        PageSize = 512,

                    }),
                };
            }, logger);
            await manager.InitializeAsync();
            var client = manager.GetOrCreateClient("test");
            var tree = await client.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });

            for (int i = 0; i < 4096; i++)
            {
                await tree.Upsert(i, "hello");
            }
            
            await tree.Commit();
            await manager.CheckpointAsync();
            manager.ClearCache();

            device.readFailure = true;
            

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await tree.GetValue(7);
            });
        }
    }
}
