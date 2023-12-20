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

using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FASTER.core;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using Microsoft.Extensions.Logging.Abstractions;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeTests : IDisposable
    {
        private IBPlusTree<long, string> _tree;
        StateManager.StateManagerSync stateManager;
        public BPlusTreeTests()
        {
            _tree = Init().GetAwaiter().GetResult();
        }

        private async Task<IBPlusTree<long, string>> Init()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"));
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });
            return tree;
        }

        [Fact]
        public async Task TestInsert()
        {
            for (int i = 0; i < 10; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }
            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach(var page in it)
            {
                foreach(var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(10, count);
        }

        [Fact]
        public async Task TestDelete()
        {
            for (int i = 0; i < 10; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            await _tree.Delete(3);
            await _tree.Delete(9);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await _tree.Print());
            var it = _tree.CreateIterator();
            await it.SeekFirst();

            List<long> expected = new List<long>();
            expected.Add(0);
            expected.Add(1);
            expected.Add(2);
            expected.Add(4);
            expected.Add(5);
            expected.Add(6);
            expected.Add(7);
            expected.Add(8);

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(expected[count], kv.Key);
                    count++;
                }
            }
            Assert.Equal(8, count);
        }

        [Fact]
        public async Task TestSplitInternalNode()
        {
            for (int i = 0; i < 1000; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    var v = await _tree.GetValue(kv.Key);
                    Assert.Equal($"{kv.Key}", v.value);
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(1000, count);
        }

        [Fact]
        public async Task TestMergeInternalNodeWithRight()
        {
            for (int i = 0; i < 1000; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            for (int i = 0; i < 800; i++)
            {
                await _tree.Delete(i);
            }

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 800;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    var v = await _tree.GetValue(kv.Key);
                    Assert.Equal($"{kv.Key}", v.value);
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(1000, count);
        }

        [Fact]
        public async Task TestMergeInternalNodeWithLeft()
        {
            for (int i = 0; i < 1000; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            for (int i = 1000; i >= 200; i--)
            {
                await _tree.Delete(i);
            }

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    var v = await _tree.GetValue(kv.Key);
                    Assert.Equal($"{kv.Key}", v.value);
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(200, count);
        }

        public void Dispose()
        {
            if (stateManager != null)
            {
                stateManager.Dispose();
            }
        }


        [Fact]
        public async Task TestDistributeInternalNodes_LeftIsSmaller()
        {
            Random r = new Random(1023);

            HashSet<int> values = new HashSet<int>();
            for (int i = 0; i < 100; i++)
            {
                var val = r.Next();
                var op = r.Next();

                if (op % 8 == 0)
                {
                    if (values.Count > 0)
                    {
                        var e = values.ElementAt(r.Next(values.Count));
                        await _tree.Delete(e);
                        values.Remove(e);
                    }

                }
                else
                {
                    await _tree.Upsert(val, val.ToString());
                    values.Add(val);
                }
            }

            var printedTree = await _tree.Print();

            var sortedOrder = values.OrderBy(x => x).ToList();

            int count = 0;
            var it = _tree.CreateIterator();
            await it.SeekFirst();
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    var v = await _tree.GetValue(kv.Key);
                    Assert.Equal($"{kv.Key}", v.value);

                    var ex = sortedOrder[count];
                    Assert.Equal(ex, kv.Key);
                    count++;
                }
            }
        }

        [Fact]
        public async Task TestDistributeInternalNodes_RightIsSmaller()
        {
            Random r = new Random(35385);

            HashSet<int> values = new HashSet<int>();
            for (int i = 0; i < 100; i++)
            {
                var val = r.Next();
                var op = r.Next();

                if (op % 4 == 0)
                {
                    if (values.Count > 0)
                    {
                        var e = values.ElementAt(r.Next(values.Count));
                        await _tree.Delete(e);
                        values.Remove(e);
                    }

                }
                else
                {
                    await _tree.Upsert(val, val.ToString());
                    values.Add(val);
                }
            }

            var printedTree = await _tree.Print();

            var sortedOrder = values.OrderBy(x => x).ToList();

            int count = 0;
            var it = _tree.CreateIterator();
            await it.SeekFirst();
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    var v = await _tree.GetValue(kv.Key);
                    Assert.Equal($"{kv.Key}", v.value);

                    var ex = sortedOrder[count];
                    if (ex != kv.Key)
                    {
                        v = await _tree.GetValue(sortedOrder[count]);
                    }
                    Assert.Equal(ex, kv.Key);
                    count++;
                }
            }
        }

        [Fact]
        public async Task TestDistributeInternalNodes_RightIsSmallerMoveMultipleLeafs()
        {
            Random r = new Random(19145);

            HashSet<int> values = new HashSet<int>();
            for (int i = 0; i < 200; i++)
            {
                var val = r.Next();
                var op = r.Next();

                if (op % 4 == 0)
                {
                    if (values.Count > 0)
                    {
                        var e = values.ElementAt(r.Next(values.Count));
                        await _tree.Delete(e);
                        values.Remove(e);
                    }

                }
                else
                {
                    await _tree.Upsert(val, val.ToString());
                    values.Add(val);
                }
            }

            var printedTree = await _tree.Print();

            var sortedOrder = values.OrderBy(x => x).ToList();

            int count = 0;
            var it = _tree.CreateIterator();
            await it.SeekFirst();
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    var v = await _tree.GetValue(kv.Key);
                    Assert.Equal($"{kv.Key}", v.value);

                    var ex = sortedOrder[count];
                    if (ex != kv.Key)
                    {
                        v = await _tree.GetValue(sortedOrder[count]);
                    }
                    Assert.Equal(ex, kv.Key);
                    count++;
                }
            }
        }

        [Fact]
        public async Task TestDistributeInternalWithInternalChildren()
        {
            Random r = new Random(176080);

            HashSet<int> values = new HashSet<int>();
            for (int i = 0; i < 400; i++)
            {
                var val = r.Next();


                var op = r.Next();
                if (op % 4 == 0)
                {
                    if (values.Count > 0)
                    {
                        var e = values.ElementAt(r.Next(values.Count));
                        await _tree.Delete(e);
                        values.Remove(e);
                        var v = await _tree.GetValue(746493011);
                    }
                }
                else
                {
                    await _tree.Upsert(val, val.ToString());
                    values.Add(val);
                }
                var printedTree = await _tree.Print();
                var sortedOrder = values.OrderBy(x => x).ToList();
                var it = _tree.CreateIterator();
                await it.SeekFirst();
                int count = 0;
                await foreach (var page in it)
                {
                    foreach (var kv in page)
                    {
                        var v = await _tree.GetValue(kv.Key);
                        Assert.Equal($"{kv.Key}", v.value);

                        var ex = sortedOrder[count];
                        if (ex != kv.Key)
                        {
                            v = await _tree.GetValue(sortedOrder[count]);
                        }
                        Assert.Equal(ex, kv.Key);
                        count++;
                    }
                }
            }
        }
    }
}
