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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    internal class GenericDataTestStream : FlowtideTestStream
    {
        private readonly TestDataSource testDataSource;

        public GenericDataTestStream(TestDataSource testDataSource, string testName) : base(testName)
        {
            this.testDataSource = testDataSource;
        }

        protected override void AddReadResolvers(ReadWriteFactory factory)
        {
            factory.AddGenericDataSource("*", (rel) => testDataSource);
        }
    }

    internal class TestDataSource : GenericDataSource<User>
    {
        private readonly List<FlowtideGenericObject<User>> _changes = new List<FlowtideGenericObject<User>>();
        private readonly TimeSpan? deltaTime;
        private int _index = 0;

        public TestDataSource(TimeSpan? deltaTime)
        {
            this.deltaTime = deltaTime;
        }

        public void AddChange(FlowtideGenericObject<User> change)
        {
            _changes.Add(change);
        }

        public void ClearChanges()
        {
            _changes.Clear();
        }

        public override TimeSpan? DeltaLoadInterval => deltaTime;

        protected override IEnumerable<FlowtideGenericObject<User>> DeltaLoad(long lastWatermark)
        {
            for (; _index < _changes.Count; _index++)
            {
                if (_changes[_index].Watermark > lastWatermark)
                {
                    yield return _changes[_index];
                }
            }
        }

        protected override IEnumerable<FlowtideGenericObject<User>> FullLoad()
        {
            _index = 0;
            for (; _index < _changes.Count; _index++)
            {
                yield return _changes[_index];
            }
        }
    }

    public class GenericReadOperatorTests
    {
        [Fact]
        public async Task TestGenericDataSource()
        {
            var source = new TestDataSource(TimeSpan.FromMilliseconds(1));
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false));

            var stream = new GenericDataTestStream(source, "TestGenericDataSource");
            stream.RegisterTableProviders(builder =>
            {
                builder.AddGenericDataTable<User>("users");
            });
            
            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    UserKey, 
                    FirstName 
                FROM users
            ");
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test" } }.Select(x => new {x.UserKey, x.FirstName}));

            // Update user 1
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test2" }, 2, false));
            source.AddChange(new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "Test3" }, 3, false));
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test2" }, new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));

            // Delete
            source.AddChange(new FlowtideGenericObject<User>("1", null, 4, true));
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));

        }

        [Fact]
        public async Task TestDeltaTrigger()
        {
            var source = new TestDataSource(default);
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false));

            var stream = new GenericDataTestStream(source, "TestDeltaTrigger");
            stream.RegisterTableProviders(builder =>
            {
                builder.AddGenericDataTable<User>("users");
            });

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    UserKey, 
                    FirstName 
                FROM users
            ");
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test" } }.Select(x => new { x.UserKey, x.FirstName }));

            // Update user 1
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test2" }, 2, false));
            source.AddChange(new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "Test3" }, 3, false));

            await stream.Trigger("delta_load");
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test2" }, new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));

            source.AddChange(new FlowtideGenericObject<User>("1", null, 4, true));
            await stream.Trigger("delta_load_users");
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));

        }

        [Fact]
        public async Task TestEmit()
        {
            var source = new TestDataSource(default);
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test", LastName = "last1" }, 1, false));
            source.AddChange(new FlowtideGenericObject<User>("3", new User { UserKey = 3, FirstName = "Test3", LastName = "last3" }, 1, false));

            var stream = new GenericDataTestStream(source, "TestEmit");
            stream.RegisterTableProviders(builder =>
            {
                builder.AddGenericDataTable<User>("users");
            });

            await stream.StartStream(@"
                CREATE VIEW v AS
                SELECT 
                    FirstName,
                    LastName
                FROM users
                WHERE UserKey = 1;

                INSERT INTO output
                SELECT 
                    FirstName,
                    LastName
                FROM v
            ");
            await stream.WaitForUpdate();

            var act = stream.GetActualRowsAsVectors();
            stream.AssertCurrentDataEqual(new List<User>() { new User { FirstName = "Test", LastName = "last1" } }.Select(x => new { x.FirstName, x.LastName }));

            // Update user 1
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test2", LastName = "last1" }, 2, false));
            source.AddChange(new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "Test3", LastName = "last2" }, 3, false));

            await stream.Trigger("delta_load");
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { FirstName = "Test2", LastName = "last1" } }.Select(x => new { x.FirstName, x.LastName }));

            source.ClearChanges();
            await stream.Trigger("full_load");
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(new List<User>().Select(x => new { x.FirstName, x.LastName }));

        }
    }
}
