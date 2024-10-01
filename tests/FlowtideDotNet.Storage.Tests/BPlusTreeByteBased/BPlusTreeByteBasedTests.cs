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
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BPlusTreeByteBased
{
    public class BPlusTreeByteBasedTests
    {

        private async Task<IBPlusTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>> Init()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            var stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>("tree",
                new Tree.BPlusTreeOptions<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new ListWithSizeComparer(new LongComparer()),
                    KeySerializer = new ListKeyWithSizeSerializer(17000),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    UseByteBasedPageSizes = true
                });
            return tree;
        }


        /// <summary>
        /// This test checks that the tree functions even when a single row
        /// extends the max size of a page.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestElementLargerThanPageSize()
        {
            var tree = await Init();
            for (int i = 0; i < 41; i++)
            {
                await tree.Upsert(new KeyValuePair<long, long>(i, 33000), $"{i}");
            }
            var it = tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key.Key);
                    count++;
                }
            }
            for (int i = 0; i < 41; i++)
            {
                await it.Seek(new KeyValuePair<long, long>(i, 17000));
                var asyncEnum = it.GetAsyncEnumerator();
                Assert.True(await asyncEnum.MoveNextAsync());
                Assert.Equal(i, asyncEnum.Current.First().Key.Key);
            }
            Assert.Equal(41, count);
        }

        [Fact]
        public async Task InsertLargerThanPageSizeMillion()
        {
            var tree = await Init();
            for (int i = 0; i < 1_000_000; i++)
            {
                await tree.Upsert(new KeyValuePair<long, long>(i, 33000), $"{i}");
            }
            var it = tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key.Key);
                    count++;
                }
            }
            for (int i = 0; i < 1_000_000; i++)
            {
                await it.Seek(new KeyValuePair<long, long>(i, 17000));
                var asyncEnum = it.GetAsyncEnumerator();
                Assert.True(await asyncEnum.MoveNextAsync());
                Assert.Equal(i, asyncEnum.Current.First().Key.Key);
            }
            Assert.Equal(1_000_000, count);
        }

        [Fact]
        public async Task RandomOperations()
        {
            List<int> insertedElements = new List<int>();
            var tree = await Init();
            var rand = new Random(123);
            // Issue happens before 149
            // Page 50 is merged but has been copied over to another node
            // 66 is bad

            //KeyValuePair<long, long> asd = new KeyValuePair<long, long>(1, 3);
            //asd.ToString();
            for (int i = 0; i < 1_000_00; i++)
            {
                //if (i == 37)
                //{

                //}
                try
                {
                    var operation = rand.Next(2);
                    switch (operation)
                    {
                        case 0:
                            var elementId = rand.Next(1_000_000);
                            var ind = insertedElements.BinarySearch(elementId);
                            if (ind < 0)
                            {
                                insertedElements.Insert(~ind, elementId);
                            }
                            await tree.Upsert(new KeyValuePair<long, long>(elementId, 33000), $"{elementId}");
                            break;
                        case 1:
                            if (insertedElements.Count == 0)
                            {
                                continue;
                            }
                            var elementIndex = rand.Next(insertedElements.Count);
                            var element = insertedElements[elementIndex];
                            await tree.Delete(new KeyValuePair<long, long>(element, 33000));
                            insertedElements.RemoveAt(elementIndex);
                            break;
                    }
                    //await tree.Upsert(i, $"{i}");
                }
                catch(Exception e)
                {
                    throw;
                }
            }

            var printed = await tree.Print();
            //var it = tree.CreateIterator();
            //await it.SeekFirst();

            //int count = 0;
            //await foreach (var page in it)
            //{
            //    foreach (var kv in page)
            //    {
            //        Assert.Equal(count, kv.Key);
            //        count++;
            //    }
            //}
            //for (int i = 0; i < 1_000_000; i++)
            //{
            //    await it.Seek(i);
            //    var asyncEnum = it.GetAsyncEnumerator();
            //    Assert.True(await asyncEnum.MoveNextAsync());
            //    Assert.Equal(i, asyncEnum.Current.First().Key);
            //}
            //Assert.Equal(1_000_000, count);
        }

        private static int GetNormalSize(Random rand, int mean, int stdDev)
        {
            double u1 = 1.0 - rand.NextDouble(); //uniform(0,1] random doubles
            double u2 = 1.0 - rand.NextDouble();
            double randStdNormal = Math.Sqrt(-2.0 * Math.Log(u1)) *
                         Math.Sin(2.0 * Math.PI * u2); //random normal(0,1)
            double randNormal =
                         mean + stdDev * randStdNormal;

            if (randNormal < 0.0)
            {
                randNormal = 1;
            }
            return (int)randNormal;
        }

        [Fact]
        public async Task RandomOperationsRandomSize()
        {
            List<int> insertedElements = new List<int>();
            var tree = await Init();
            var rand = new Random(123);
            // Issue happens before 149
            // Page 50 is merged but has been copied over to another node
            // 66 is bad

            //KeyValuePair<long, long> asd = new KeyValuePair<long, long>(1, 3);
            //asd.ToString();
            for (int i = 0; i < 100; i++)
            {
                //if (i == 37)
                //{

                //}
                try
                {
                    var operation = rand.Next(1);
                    switch (operation)
                    {
                        case 0:
                            var elementId = i; //rand.Next(1_000_000);
                            var ind = insertedElements.BinarySearch(elementId);
                            if (ind < 0)
                            {
                                insertedElements.Insert(~ind, elementId);
                            }
                            var size = GetNormalSize(rand, 100, 5000); //rand.Next(1, 16000);
                            await tree.Upsert(new KeyValuePair<long, long>(elementId, i * 20), $"{elementId}");
                            break;
                        //case 1:
                        //    if (insertedElements.Count == 0)
                        //    {
                        //        continue;
                        //    }
                        //    var elementIndex = rand.Next(insertedElements.Count);
                        //    var element = insertedElements[elementIndex];
                        //    await tree.Delete(new KeyValuePair<long, long>(element, 33000));
                        //    insertedElements.RemoveAt(elementIndex);
                        //    break;
                    }
                    //await tree.Upsert(i, $"{i}");
                }
                catch (Exception e)
                {
                    throw;
                }
            }

            while (insertedElements.Count > 100)
            {
                var elementIndex = rand.Next(insertedElements.Count);
                var element = insertedElements[elementIndex];
                await tree.Delete(new KeyValuePair<long, long>(element, 33000));
                insertedElements.RemoveAt(elementIndex);
            }

            var printed = await tree.Print();
            //var it = tree.CreateIterator();
            //await it.SeekFirst();

            //int count = 0;
            //await foreach (var page in it)
            //{
            //    foreach (var kv in page)
            //    {
            //        Assert.Equal(count, kv.Key);
            //        count++;
            //    }
            //}
            //for (int i = 0; i < 1_000_000; i++)
            //{
            //    await it.Seek(i);
            //    var asyncEnum = it.GetAsyncEnumerator();
            //    Assert.True(await asyncEnum.MoveNextAsync());
            //    Assert.Equal(i, asyncEnum.Current.First().Key);
            //}
            //Assert.Equal(1_000_000, count);
        }
    }
}
