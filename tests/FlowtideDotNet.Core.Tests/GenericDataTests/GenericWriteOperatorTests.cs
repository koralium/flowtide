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
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Sources.Generic;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    internal class GenericDataSinkTestStream : FlowtideTestStream
    {
        private readonly TestDataSink testDataSink;

        public GenericDataSinkTestStream(TestDataSink testDataSink, string testName) : base(testName)
        {
            this.testDataSink = testDataSink;
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            factory.AddCustomSink("output", (rel) => testDataSink, Core.Operators.Write.ExecutionMode.OnCheckpoint);
        }
    }

    internal class TestDataSink : GenericDataSink<User>
    {
        public Dictionary<int, User> users = new Dictionary<int, User>();
        public int changeCounter = 0;

        public override Task<List<string>> GetPrimaryKeyNames()
        {
            return Task.FromResult(new List<string> { "UserKey" });
        }

        public override async Task OnChanges(IAsyncEnumerable<FlowtideGenericWriteObject<User>> changes, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var userChange in changes)
            {
                if (!userChange.IsDeleted)
                {
                    users[userChange.Value.UserKey] = userChange.Value;
                }
                else
                {
                    users.Remove(userChange.Value.UserKey);
                }
            }
            changeCounter++;
        }
    }

    public class GenericWriteOperatorTests
    {
        [Fact]
        public async Task TestGenericDataSink()
        {
            var sink = new TestDataSink();
            GenericDataSinkTestStream stream = new GenericDataSinkTestStream(sink, "testgenericsink");

            stream.Generate();

            await stream.StartStream(@"
                INSERT INTO output
                SELECT userKey, firstName, active
                FROM users
            ");

            while (true)
            {
                if (sink.changeCounter > 0)
                {
                    break;
                }
                await Task.Delay(10);
                await stream.SchedulerTick();
            }

            Assert.Equal(stream.Users.Select(x => new User() { UserKey = x.UserKey, FirstName = x.FirstName, Active = x.Active }).OrderBy(x => x.UserKey), sink.users.Values.OrderBy(x => x.UserKey));

            var firstUser = stream.Users[0];
            stream.DeleteUser(firstUser);

            while (true)
            {
                if (sink.changeCounter > 1)
                {
                    break;
                }
                await Task.Delay(10);
                await stream.SchedulerTick();
            }

            Assert.Equal(stream.Users.Select(x => new User() { UserKey = x.UserKey, FirstName = x.FirstName, Active = x.Active }).OrderBy(x => x.UserKey), sink.users.Values.OrderBy(x => x.UserKey));
        }
    }
}
