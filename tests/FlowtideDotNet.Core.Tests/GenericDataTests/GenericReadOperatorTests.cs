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
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    internal class GenericDataTestStream<T> : FlowtideTestStream
        where T: class
    {
        private readonly GenericDataSourceAsync<T> testDataSource;

        public GenericDataTestStream(GenericDataSourceAsync<T> testDataSource, string testName) : base(testName)
        {
            this.testDataSource = testDataSource;
        }

        protected override void AddReadResolvers(IConnectorManager manager)
        {
            manager.AddCustomSource("users", (rel) => testDataSource);
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

        public override IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield return new EnumResolver(enumAsStrings: true);
        }
    }

    internal class TestDataSourceWithLookup : GenericDataSourceAsync<User>
    {
        private readonly List<FlowtideGenericObject<User>> _changes = new List<FlowtideGenericObject<User>>();
        private readonly TimeSpan? deltaTime;
        private int _index = 0;

        public TestDataSourceWithLookup(TimeSpan? deltaTime)
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

        public override async IAsyncEnumerable<FlowtideGenericObject<User>> DeltaLoadAsync(long lastWatermark)
        {
            for (; _index < _changes.Count; _index++)
            {
                if (_changes[_index].Watermark > lastWatermark)
                {
                    var lookedUpRowBefore = await LookupRow(_changes[_index].Key);
                    if (lookedUpRowBefore != null && !_changes[_index].isDelete)
                    {
                        _changes[_index].Value!.FirstName = lookedUpRowBefore.FirstName + "Updated";
                    }
                    
                    yield return _changes[_index];
                }
            }
        }

        public override Task Checkpoint()
        {
            return base.Checkpoint();
        }

        public override IAsyncEnumerable<FlowtideGenericObject<User>> FullLoadAsync()
        {
            return _changes.ToAsyncEnumerable();
        }

        public override IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield return new EnumResolver(enumAsStrings: true);
        }
    }

    public class GenericReadOperatorTests
    {
        [Fact]
        public async Task TestGenericDataSource()
        {
            var source = new TestDataSource(TimeSpan.FromMilliseconds(1));
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false));

            var stream = new GenericDataTestStream<User>(source, "TestGenericDataSource");
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

            var stream = new GenericDataTestStream<User>(source, "TestDeltaTrigger");
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

            var stream = new GenericDataTestStream<User>(source, "TestEmit");
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

        [Fact]
        public async Task TestSelectKey()
        {
            var source = new TestDataSource(default);
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test", LastName = "last1" }, 1, false));
            source.AddChange(new FlowtideGenericObject<User>("3", new User { UserKey = 3, FirstName = "Test3", LastName = "last3" }, 1, false));

            var stream = new GenericDataTestStream<User>(source, "TestSelectKey");
            stream.RegisterTableProviders(builder =>
            {
                builder.AddGenericDataTable<User>("users");
            });

            await stream.StartStream(@"
                CREATE VIEW v AS
                SELECT 
                    FirstName,
                    __key,
                    LastName
                FROM users
                WHERE UserKey = 1;

                INSERT INTO output
                SELECT
                    FirstName,
                    __key,
                    LastName
                FROM v
            ");
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test", LastName = "last1" } }.Select(x => new { x.FirstName, key = x.UserKey.ToString(), x.LastName }));
        }

        [Fact]
        public async Task TestGenericDataSourceWithLookup()
        {
            var source = new TestDataSourceWithLookup(TimeSpan.FromMilliseconds(1));
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false));

            var stream = new GenericDataTestStream<User>(source, "TestGenericDataSourceWithLookup");
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
            await stream.WaitForUpdate();

            // Check that the old name is set with the extra updated to make sure that it used lookuprow
            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "TestUpdated" }, new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));

            // Delete
            source.AddChange(new FlowtideGenericObject<User>("1", null, 4, true));
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));
        }

        [Fact]
        public async Task TestGenericDataSourceWithCustomLocking()
        {
            var source = new TestDataSourceWithCustomLocking();
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false));

            var stream = new GenericDataTestStream<User>(source, "TestGenericDataSourceWithCustomLocking");
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

            // Trigger the delta load trigger once to start the background reactive wait loop.
            // This is needed because the DeltaLoadInterval is null, so it's not run automatically.
            await stream.Trigger("delta_load");

            // Update user 1
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test2" }, 2, false));
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test2" } }.Select(x => new { x.UserKey, x.FirstName }));

            // Add another change
            source.AddChange(new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "Test3" }, 3, false));
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>() { new User { UserKey = 1, FirstName = "Test2" }, new User { UserKey = 2, FirstName = "Test3" } }.Select(x => new { x.UserKey, x.FirstName }));
        }

        [Fact]
        public async Task TestGenericDataSourceErrorPropagation()
        {
            var source = new TestDataSourceWithException();
            var stream = new GenericDataTestStream<User>(source, "TestGenericDataSourceErrorPropagation");
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

            await stream.Trigger("delta_load");

            source.TriggerError();

            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await stream.WaitForUpdate();
            });

            var inner = exception;
            if (exception is AggregateException agg)
            {
                inner = agg.Flatten().InnerException!;
            }
            Assert.IsType<InvalidOperationException>(inner);
            Assert.Equal("Simulated error in RunDeltaLoad", inner.Message);
        }
    }

    internal class TestDataSourceWithCustomLocking : GenericDataSourceAsync<User>
    {
        private readonly List<FlowtideGenericObject<User>> _changes = new List<FlowtideGenericObject<User>>();
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0);
        private int _index = 0;

        public void AddChange(FlowtideGenericObject<User> change)
        {
            lock (_changes)
            {
                _changes.Add(change);
            }
            _semaphore.Release();
        }

        public override bool IsPushBased => true;

        public override TimeSpan? DeltaLoadInterval => default;

        public override async Task RunDeltaLoad(IDeltaLoadContext<User> context, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await _semaphore.WaitAsync(cancellationToken);

                await using var tx = await context.BeginTransactionAsync();

                List<FlowtideGenericObject<User>> localChanges;
                lock (_changes)
                {
                    localChanges = new List<FlowtideGenericObject<User>>(_changes);
                }

                for (; _index < localChanges.Count; _index++)
                {
                    await tx.SubmitAsync(localChanges[_index]);
                }
            }
        }

        public override IAsyncEnumerable<FlowtideGenericObject<User>> FullLoadAsync()
        {
            List<FlowtideGenericObject<User>> snapshot;
            lock (_changes)
            {
                snapshot = new List<FlowtideGenericObject<User>>(_changes);
            }
            return snapshot.ToAsyncEnumerable();
        }

        public override Task Checkpoint()
        {
            return base.Checkpoint();
        }

        public override IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield return new EnumResolver(enumAsStrings: true);
        }
    }

    internal class TestDataSourceWithException : GenericDataSourceAsync<User>
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0);

        public void TriggerError()
        {
            _semaphore.Release();
        }

        public override bool IsPushBased => true;

        public override TimeSpan? DeltaLoadInterval => default;

        public override async Task RunDeltaLoad(IDeltaLoadContext<User> context, CancellationToken cancellationToken)
        {
            await _semaphore.WaitAsync(cancellationToken);
            throw new InvalidOperationException("Simulated error in RunDeltaLoad");
        }

        public override IAsyncEnumerable<FlowtideGenericObject<User>> FullLoadAsync()
        {
            // Return a single user
            return new List<User>() { new User() { UserKey = 1, FirstName = "Test" } }.Select(u => new FlowtideGenericObject<User>(u.UserKey.ToString(), u, 1, false)).ToAsyncEnumerable();
        }
    }
}
