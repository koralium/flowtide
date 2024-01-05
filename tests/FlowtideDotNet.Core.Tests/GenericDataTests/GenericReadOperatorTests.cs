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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            factory.AddGenericDataSource("*", testDataSource);
        }
    }

    internal class TestDataSource : GenericDataSource<User>
    {
        private readonly List<FlowtideGenericObject<User>> _changes = new List<FlowtideGenericObject<User>>();
        private int _index = 0;
        public void AddChange(FlowtideGenericObject<User> change)
        {
            _changes.Add(change);
        }
        public override TimeSpan? DeltaLoadInterval => TimeSpan.FromSeconds(1);

        public override async IAsyncEnumerable<FlowtideGenericObject<User>> DeltaLoadAsync(long lastWatermark)
        {
            for (; _index < _changes.Count; _index++)
            {
                if (_changes[_index].Watermark > lastWatermark)
                {
                    yield return _changes[_index];
                }
            }
        }

        public override async IAsyncEnumerable<FlowtideGenericObject<User>> FullLoadAsync()
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
            var source = new TestDataSource();
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false));

            var stream = new GenericDataTestStream(source, "TestGenericDataSource");
            stream.Generate();

            
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
    }
}
