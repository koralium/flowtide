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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    internal class ImmutableGenericDataTestStream<T> : FlowtideTestStream
        where T : class
    {
        private readonly ImmutableGenericDataSourceAsync<T> testDataSource;

        public ImmutableGenericDataTestStream(ImmutableGenericDataSourceAsync<T> testDataSource, string testName) : base(testName)
        {
            this.testDataSource = testDataSource;
        }

        protected override void AddReadResolvers(IConnectorManager manager)
        {
            manager.AddCustomImmutableSource("users", (rel) => testDataSource);
        }
    }

    internal class ImmutableTestDataSource : ImmutableGenericDataSourceAsync<User>
    {
        private readonly List<User> _changes = new List<User>();
        private readonly TimeSpan? deltaTime;
        private int _index = 0;

        public ImmutableTestDataSource(TimeSpan? deltaTime)
        {
            this.deltaTime = deltaTime;
        }

        public void AddChange(User change)
        {
            _changes.Add(change);
        }

        public override IAsyncEnumerable<ImmutableRow<User>> FullLoadAsync()
        {
            return _changes.Select(c => new ImmutableRow<User>(c, c.UserKey)).ToAsyncEnumerable();
        }

        public override IAsyncEnumerable<ImmutableRow<User>> DeltaLoadAsync(long lastWatermark)
        {
            return _changes.Where(c => c.UserKey > lastWatermark).Select(c => new ImmutableRow<User>(c, c.UserKey)).ToAsyncEnumerable();
        }

        public override TimeSpan? DeltaLoadInterval => deltaTime;

    }

    public class ImmutableGenericReadOperatorTests
    {
        [Fact]
        public async Task TestImmutableGenericDataSource()
        {
            var source = new ImmutableTestDataSource(TimeSpan.FromMilliseconds(1));
            source.AddChange(new User { UserKey = 1, FirstName = "Test" });

            var stream = new ImmutableGenericDataTestStream<User>(source, "TestImmutableGenericDataSource");
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

            source.AddChange(new User { UserKey = 2, FirstName = "Test3" });
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test" }, new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));


        }
    }
}
