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
using FASTER.core;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class CheckpointTests
    {
        [Fact]
        public async Task TestCheckpoint()
        {
            var device = Devices.CreateLogDevice("./data/tmp/persistent");
            StateManager.StateManagerSync stateManager = new StateManager.StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
                    {
                        LogDevice = device,
                        CheckpointDir = "./data/tmp/persistent"
                    })
                }, NullLogger.Instance, new Meter($"storage"), "storage");

            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });

            await tree.Upsert(1, "hello");

            await tree.Commit();

            await stateManager.CheckpointAsync();

            
            await tree.Upsert(1, "helloOther");

            var (found, val) = await tree.GetValue(1);
            Assert.Equal("helloOther", val);
            
            // Restore
            await stateManager.InitializeAsync();

            await tree.Upsert(1, "helloOther");
            (found, val) = await tree.GetValue(1);
            Assert.Equal("helloOther", val);

            // Commit data and take only checkpoint without metadata, this is to force written data
            await tree.Commit();

            await stateManager.InitializeAsync();

            (found, val) = await tree.GetValue(1);

            Assert.Equal("hello", val);
  ;     }

        [Fact]
        public async Task TestFailureAfterNewRootInBPlusTree()
        {
            var device = Devices.CreateLogDevice("./data/tmp/persistentfail");
            StateManager.StateManagerSync stateManager = new StateManager.StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
                    {
                        LogDevice = device,
                        CheckpointDir = "./data/tmp/persistentfail"
                    })
                }, NullLogger.Instance, new Meter($"storage"), "storage");

            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });

            await tree.Upsert(0, "hello0");

            await tree.Commit();

            var printedFirst = await tree.Print();
            var beforeCheckPointUrl = KrokiUrlBuilder.ToKrokiUrl(printedFirst);

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJw0TjEOwyAM3CPlDxZz1STtWOAjUQcSXEBCgAApQ9W_15B28Mnnu7OtnckqWTDwHoeSvAtYRMNxCFEjrMWqhCAgxIAXi87YKq7L83Hq99WrDT3pnFe1eYQtZo1ZsJnBjt7_6XLSktTugmmy5DVTaTnziZA6SDFXwV4UzfEgaxDsRr4uT919Jixtir9Yn0_9tpTtrc84fAEAAP__AwA_eUQ8",
                beforeCheckPointUrl
                );
            await stateManager.CheckpointAsync();

            for (int i = 1; i < 9; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }

            var printed = await tree.Print();
            var afterCheckPointUrl = KrokiUrlBuilder.ToKrokiUrl(printed);

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJysUMFuhCAUvJv4Dy-c26rr2jat8COmB1QUEwIGSPaw6b8XUCF78NL2IHnzJs7Mm3GZNV05zHDPM7OKRTKD_ZtnUo0MOsPpygCDVJI9cbbM3OKX6utz46-doD0Tjm9bS3vBoFd6ZBqjEsHAhDhgtUGz0mGRs6dJa7X7RliVthhNflPY0a9IfUwHWR1k4X8qghUhR4r67ylIGc2rOF3OAzlhrW5OSGJ0eQi363Hno5JogEk5wCQfYH1yIQpFo4-phGcCAdVoO7z5h8OvMUUTp9c4vcXp_XcVJPkAk0eAySjA5BbgbnneSBUbaVwjezWPa-gGJY3VdJEWT1QY5hW-8-wHAAD__wMAlZvwxQ==",
                afterCheckPointUrl
                );

            // Restore
            await stateManager.InitializeAsync();

            var printedAfter = await tree.Print();
            var afterRestoreUrl = KrokiUrlBuilder.ToKrokiUrl(printedAfter);

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJw0TjEOwyAM3CPlDxZz1STtWOAjUQcSXEBCgAApQ9W_15B28Mnnu7OtnckqWTDwHoeSvAtYRMNxCFEjrMWqhCAgxIAXi87YKq7L83Hq99WrDT3pnFe1eYQtZo1ZsJnBjt7_6XLSktTugmmy5DVTaTnziZA6SDFXwV4UzfEgaxDsRr4uT919Jixtir9Yn0_9tpTtrc84fAEAAP__AwA_eUQ8",
                afterRestoreUrl
                );
        }

        [Fact]
        public async Task TestCheckpointWithCompactionRestore()
        {
            var device = Devices.CreateLogDevice("./data/tmp/persistentcompact");
            StateManager.StateManagerSync stateManager = new StateManager.StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
                    {
                        LogDevice = device,
                        CheckpointDir = "./data/tmp/persistentcompact"
                    })
                },  NullLogger.Instance, new Meter($"storage"), "storage");

            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });

            await tree.Upsert(1, "hello");

            await tree.Commit();

            await stateManager.CheckpointAsync();

            var (foundInitial, valInitial) = await tree.GetValue(1);
            await stateManager.Compact();

            await tree.Upsert(1, "helloOther");

            var (found, val) = await tree.GetValue(1);
            Assert.Equal("helloOther", val);

            // Restore
            await stateManager.InitializeAsync();
            (found, val) = await tree.GetValue(1);

            await tree.Upsert(1, "helloOther");
            (found, val) = await tree.GetValue(1);
            Assert.Equal("helloOther", val);

            // Commit data and take only checkpoint without metadata, this is to force written data
            await tree.Commit();

            await stateManager.InitializeAsync();

            (found, val) = await tree.GetValue(1);

            Assert.Equal("hello", val);
            ;
        }
    }
}
