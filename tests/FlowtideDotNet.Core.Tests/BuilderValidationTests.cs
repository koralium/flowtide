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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Core.Tests.Failure;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests
{
    public class BuilderValidationTests
    {
        [Fact]
        public void TestNoPlan()
        {
            var e = Assert.Throws<InvalidOperationException>(() =>
            {
                var stream = new FlowtideBuilder("test")
                    .Build();
            });
            Assert.Equal("No plan has been added.", e.Message);
        }

        [Fact]
        public void TestNoReadWriteFactory()
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("a", new List<string>() { "c1" });
            builder.Sql("INSERT INTO test SELECT c1 FROM a");
            var plan = builder.GetPlan();

            var e = Assert.Throws<InvalidOperationException>(() =>
            {
                var stream = new FlowtideBuilder("test")
                    .AddPlan(plan)
                    .Build();
            });
            Assert.Equal("No read write factory has been added.", e.Message);
        }

        [Fact]
        public void TestNoSuitableReadResolver()
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("a", new List<string>() { "c1" });
            builder.Sql("INSERT INTO test SELECT c1 FROM a");
            var plan = builder.GetPlan();

            var factory = new ReadWriteFactory();
            factory.AddConsoleSink(".*");

            var e = Assert.Throws<FlowtideException>(() =>
            {
                var stream = new FlowtideBuilder("test")
                    .AddPlan(plan)
                    .AddReadWriteFactory(factory)
                    .Build();
            });
            Assert.Equal("No read resolver matched the read relation.", e.Message);
        }

        [Fact]
        public void TestNoSuitableWriteResolver()
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("a", new List<string>() { "c1" });
            builder.Sql("INSERT INTO test SELECT c1 FROM a");
            var plan = builder.GetPlan();

            var factory = new ReadWriteFactory();
            factory.AddReadResolver((rel, opt) =>
            {
                return new ReadOperatorInfo(new FailureIngress(opt));
            });

            var e = Assert.Throws<FlowtideException>(() =>
            {
                var stream = new FlowtideBuilder("test")
                    .AddPlan(plan)
                    .AddReadWriteFactory(factory)
                    .Build();
            });
            Assert.Equal("No write resolver matched the read relation.", e.Message);
        }

        [Fact]
        public async Task ValidateSamePlan()
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("a", new List<string>() { "c1" });
            builder.Sql("INSERT INTO test SELECT c1 FROM a");
            var plan = builder.GetPlan();

            int checkpointCount = 0;
            var factory = new ReadWriteFactory();
            factory.AddWriteResolver((rel, opt) =>
            {
                return new FailureEgress(opt, new FailureEgressOptions()
                {
                    OnCompaction = () =>
                    {
                        checkpointCount++;
                    }
                });
            });
            factory.AddReadResolver((rel, opt) =>
            {
                return new ReadOperatorInfo(new TestIngress(opt));
            });

            var cache = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions());
            var stream = new FlowtideBuilder("test")
                    .AddPlan(plan)
                    .AddReadWriteFactory(factory)
                    .WithStateOptions(() => new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
                    {
                        PersistentStorage = cache
                    })
                    .Build();
            await stream.StartAsync();
            while (stream.Status != Base.Engine.StreamStatus.Running || checkpointCount == 0)
            {
                await Task.Delay(10);
            }

            SqlPlanBuilder builder2 = new SqlPlanBuilder();
            builder2.AddTableDefinition("a", new List<string>() { "c1" });
            builder2.Sql("INSERT INTO test2 SELECT c1 FROM a");
            var plan2 = builder2.GetPlan();

            var stream2 = new FlowtideBuilder("test")
                    .AddPlan(plan2)
                    .AddReadWriteFactory(factory)
                    .WithStateOptions(() => new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
                    {
                        PersistentStorage = cache
                    })
                    .Build();


            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await stream2.StartAsync();
            });
            Assert.Equal("Stream plan hash stored in storage is different than the hash used.", ex.Message);
            
        }
    }
}