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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    internal class GenericDataSinkTestStream : FlowtideTestStream
    {
        private readonly TestDataSink testDataSink;
        private readonly Core.Operators.Write.ExecutionMode executionMode;

        public GenericDataSinkTestStream(TestDataSink testDataSink, string testName, Core.Operators.Write.ExecutionMode executionMode = Core.Operators.Write.ExecutionMode.OnCheckpoint) : base(testName)
        {
            this.testDataSink = testDataSink;
            this.executionMode = executionMode;
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            factory.AddCustomSink("output", (rel) => testDataSink, executionMode);
        }
    }

    internal class TestDataSink : GenericDataSink<User>
    {
        public Dictionary<int, User> users = new Dictionary<int, User>();
        public int changeCounter = 0;
        private List<User>? _existing;

        public override Task<List<string>> GetPrimaryKeyNames()
        {
            return Task.FromResult(new List<string> { "UserKey" });
        }

        public void AddExistingUsers(List<User> existingUsers)
        {
            _existing = existingUsers;
            foreach (var user in existingUsers)
            {
                this.users[user.UserKey] = user;
            }
        }

        public override IAsyncEnumerable<User> GetExistingData()
        {
            if (_existing != null)
            {
                return _existing.ToAsyncEnumerable();
            }
            return base.GetExistingData();
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

        [Fact]
        public async Task TestGenericDataSinkExistingUsers()
        {
            var sink = new TestDataSink();
            GenericDataSinkTestStream stream = new GenericDataSinkTestStream(sink, "TestGenericDataSinkExistingUsers");

            stream.Generate(1000);

            var firstUser = stream.Users.First();
            var secondUser = stream.Users[1];
            sink.AddExistingUsers(new List<User>()
            {
                new User()
                {
                    UserKey = firstUser.UserKey,
                    FirstName = firstUser.FirstName,
                    Active = firstUser.Active
                },
                new User()
                {
                    UserKey = secondUser.UserKey,
                    FirstName = "a",
                    Active = secondUser.Active
                },
                new User()
                {
                    UserKey = int.MaxValue,
                    FirstName = "a",
                    Active = true
                }
            });

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
        }

        [Fact]
        public async Task TestGenericDataSinkWatermarkMode()
        {
            var sink = new TestDataSink();
            GenericDataSinkTestStream stream = new GenericDataSinkTestStream(sink, "TestGenericDataSinkWatermarkMode", Core.Operators.Write.ExecutionMode.OnWatermark);

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT userKey, firstName, active
                FROM users
            ");

            var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
            while (sink.changeCounter == 0)
            {
                if (DateTimeOffset.UtcNow > deadline)
                {
                    throw new TimeoutException("Timed out waiting for sink changes.");
                }
                await Task.Delay(10);
                await stream.SchedulerTick();
            }

            Assert.Equal(stream.Users.Select(x => new User() { UserKey = x.UserKey, FirstName = x.FirstName, Active = x.Active }).OrderBy(x => x.UserKey), sink.users.Values.OrderBy(x => x.UserKey));
        }

        [Fact]
        public async Task TestGenericDataSinkWatermarkModeWithExistingData()
        {
            var sink = new TestDataSink();
            GenericDataSinkTestStream stream = new GenericDataSinkTestStream(sink, "TestGenericDataSinkWatermarkModeWithExistingData", Core.Operators.Write.ExecutionMode.OnWatermark);

            stream.Generate(10);

            var firstUser = stream.Users.First();
            sink.AddExistingUsers(new List<User>()
            {
                new User()
                {
                    UserKey = firstUser.UserKey,
                    FirstName = "different_name",
                    Active = firstUser.Active
                }
            });

            await stream.StartStream(@"
                INSERT INTO output
                SELECT userKey, firstName, active
                FROM users
            ");

            var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
            while (sink.changeCounter == 0)
            {
                if (DateTimeOffset.UtcNow > deadline)
                {
                    throw new TimeoutException("Timed out waiting for sink changes.");
                }
                await Task.Delay(10);
                await stream.SchedulerTick();
            }

            Assert.Equal(stream.Users.Select(x => new User() { UserKey = x.UserKey, FirstName = x.FirstName, Active = x.Active }).OrderBy(x => x.UserKey), sink.users.Values.OrderBy(x => x.UserKey));
        }

        [Fact]
        public async Task TestEnumDeleteException()
        {
            var source = new EnumTestSource();
            var sink = new EnumTestSink();
            var stream = new EnumTestStream(source, sink, "TestEnumDeleteException");

            stream.RegisterTableProviders(builder =>
            {
                builder.AddGenericDataTable<EnumModel>("users");
            });

            // Add an insert change
            source.AddChange(new FlowtideGenericObject<EnumModel>("1", new EnumModel { Id = 1, Type = UserType.Admin }, 1, false));

            await stream.StartStream(@"
                INSERT INTO output
                SELECT Id, Type
                FROM users
            ");

            var deadline = DateTimeOffset.UtcNow.AddSeconds(1000);
            while (sink.changeCounter == 0)
            {
                if (DateTimeOffset.UtcNow > deadline)
                {
                    throw new TimeoutException("Timed out waiting for sink changes.");
                }
                await Task.Delay(10);
                await stream.SchedulerTick();
            }

            Assert.Single(sink.items);
            Assert.Equal(UserType.Admin, sink.items[1].Type);

            // Add a delete change
            source.AddChange(new FlowtideGenericObject<EnumModel>("1", null, 2, true));

            // This WaitForUpdate should throw if there is an exception when converting deleted row with enum
            deadline = DateTimeOffset.UtcNow.AddSeconds(1000);
            while (sink.changeCounter == 1)
            {
                if (DateTimeOffset.UtcNow > deadline)
                {
                    throw new TimeoutException("Timed out waiting for sink changes.");
                }
                await Task.Delay(10);
                await stream.SchedulerTick();
            }

            Assert.Empty(sink.items);
        }
    }

    public enum UserType
    {
        Admin,
        Member,
        Guest
    }

    public class EnumModel
    {
        public int Id { get; set; }
        public UserType Type { get; set; }
    }

    internal class EnumTestSource : GenericDataSource<EnumModel>
    {
        private readonly List<FlowtideGenericObject<EnumModel>> _changes = new List<FlowtideGenericObject<EnumModel>>();
        private int _index = 0;

        public void AddChange(FlowtideGenericObject<EnumModel> change)
        {
            _changes.Add(change);
        }

        public override TimeSpan? DeltaLoadInterval => TimeSpan.FromMilliseconds(1);

        protected override IEnumerable<FlowtideGenericObject<EnumModel>> DeltaLoad(long lastWatermark)
        {
            for (; _index < _changes.Count; _index++)
            {
                if (_changes[_index].Watermark > lastWatermark)
                {
                    yield return _changes[_index];
                }
            }
        }

        protected override IEnumerable<FlowtideGenericObject<EnumModel>> FullLoad()
        {
            _index = 0;
            for (; _index < _changes.Count; _index++)
            {
                yield return _changes[_index];
            }
        }

        public override IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield return new EnumResolver(enumAsStrings: true);
        }
    }

    internal class EnumTestSink : GenericDataSink<EnumModel>
    {
        public Dictionary<int, EnumModel> items = new Dictionary<int, EnumModel>();
        public int changeCounter = 0;

        public override Task<List<string>> GetPrimaryKeyNames()
        {
            return Task.FromResult(new List<string> { "Id" });
        }

        public override IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield return new EnumResolver(enumAsStrings: true);
        }

        public override async Task OnChanges(IAsyncEnumerable<FlowtideGenericWriteObject<EnumModel>> changes, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var change in changes)
            {
                if (!change.IsDeleted)
                {
                    items[change.Value.Id] = change.Value;
                }
                else
                {
                    items.Remove(change.Value.Id);
                }
            }
            changeCounter++;
        }
    }

    internal class EnumTestStream : FlowtideTestStream
    {
        private readonly EnumTestSource source;
        private readonly EnumTestSink sink;

        public EnumTestStream(EnumTestSource source, EnumTestSink sink, string testName) : base(testName)
        {
            this.source = source;
            this.sink = sink;
        }

        protected override void AddReadResolvers(IConnectorManager manager)
        {
            manager.AddCustomSource("users", (rel) => source);
        }

        protected override void AddWriteResolvers(IConnectorManager manager)
        {
            manager.AddCustomSink("output", (rel) => sink, Core.Operators.Write.ExecutionMode.OnCheckpoint);
        }
    }
}
