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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.Queue.Internal;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.Queue
{
    public class FlowtideQueueTests
    {
        private async Task<IFlowtideQueue<long, PrimitiveListValueContainer<long>>> CreateQueue(string path = "./data/temp", bool deleteOnClose = true, int cachePageCount = 1000000)
        {
            var stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = cachePageCount,
                MinCachePageCount = 0,
                PersistentStorage = new FasterKvPersistentStorage(meta => new FasterKVSettings<long, SpanByte>(path, deleteOnClose)
                {
                    PageSize = 16 * 1024 * 1024,
                    MemorySize = 32 * 1024 * 1024
                })
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var stateManagerClient = stateManager.GetOrCreateClient("test");

            return await stateManagerClient.GetOrCreateQueue("queue", new Storage.Queue.FlowtideQueueOptions<long, PrimitiveListValueContainer<long>>()
            {
                MemoryAllocator = GlobalMemoryManager.Instance,
                ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance)
            });
        }

        [Fact]
        public async Task EnqueueThenDequeueSingleElement()
        {
            var queue = await CreateQueue();

            await queue.Enqueue(1);
            Assert.Equal(1, queue.Count);
            var result = await queue.Dequeue();
            
            Assert.Equal(1, result);
            Assert.Equal(0, queue.Count);
        }

        [Fact]
        public async Task EnqueueDequeueMultipleSamePage()
        {
            var queue = await CreateQueue();

            for (int i = 0; i < 10; i++)
            {
                await queue.Enqueue(i);
            }

            for (int i = 0; i < 10; i++)
            {
                var result = await queue.Dequeue();
                Assert.Equal(i, result);
            }
        }

        [Fact]
        public async Task EnqueueDequeueMultipleAcrossPages()
        {
            var queue = await CreateQueue();

            for (int i = 0; i < 1000; i++)
            {
                await queue.Enqueue(i);
            }

            for (int i = 0; i < 1000; i++)
            {
                var result = await queue.Dequeue();
                Assert.Equal(i, result);
            }
        }

        [Fact]
        public async Task EnqueueDequeueMultipleAcrossPagesOffloadToDiskCache()
        {
            var queue = await CreateQueue(cachePageCount: 1);

            for (int i = 0; i < 1_000_000; i++)
            {
                await queue.Enqueue(i);
            }

            for (int i = 0; i < 1_000_000; i++)
            {
                var result = await queue.Dequeue();
                Assert.Equal(i, result);
            }
        }

        [Fact]
        public async Task EnqueueDequeueMultipleAcrossPagesOffloadToDiskPersistent()
        {
            var queue = await CreateQueue(cachePageCount: 1);

            for (int i = 0; i < 1_000_000; i++)
            {
                await queue.Enqueue(i);
                Assert.Equal(i + 1, queue.Count);
            }

            await queue.Commit();

            for (int i = 0; i < 1_000_000; i++)
            {
                var result = await queue.Dequeue();
                Assert.Equal(i, result);
                Assert.Equal(1_000_000 - i - 1, queue.Count);
            }
        }

        [Fact]
        public async Task ResetTest()
        {
            var queue = await CreateQueue();

            await queue.Enqueue(1);
            await queue.Enqueue(2);

            await queue.Clear();

            await queue.Enqueue(1);

            Assert.Equal(1, queue.Count);

            var result = await queue.Dequeue();
            Assert.Equal(1, result);
        }

        [Fact]
        public async Task EnqueuePopSingleElement()
        {
            var queue = await CreateQueue();
            await queue.Enqueue(1);
            var result = await queue.Pop();
            Assert.Equal(1, result);
        }

        [Fact]
        public async Task EnqueuePopAcrossPages()
        {
            var queue = await CreateQueue();

            for (int i = 0; i < 10_000; i++)
            {
                await queue.Enqueue(i);
            }
            
            for (int i = 0; i < 10_000; i++)
            {
                var result = await queue.Pop();
                Assert.Equal(10_000 - i - 1, result);
            }
            
        }
    }
}
