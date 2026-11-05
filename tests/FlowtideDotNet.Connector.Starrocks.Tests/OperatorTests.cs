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

using FlowtideDotNet.Base;
using FlowtideDotNet.Connector.StarRocks.Exceptions;
using FlowtideDotNet.Connector.StarRocks.Internal.HttpApi;
using FlowtideDotNet.Connector.StarRocks.Internal;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Tests;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.StarRocks.Tests
{
    public class OperatorTests : OperatorTestBase, IDisposable
    {
        // Default storage is shared, tests would restore each other's checkpoints
        private readonly string _directory = $"./data/starrocksOperatorTests/{Guid.NewGuid():N}";
        private FileCachePersistentStorage? _storage;

        protected override StateManagerOptions CreateStateManagerOptions()
        {
            _storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/persistent" });
            return new StateManagerOptions()
            {
                PersistentStorage = _storage,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/tmp" }
            };
        }

        void IDisposable.Dispose()
        {
            Dispose();
            _storage?.Dispose();
        }

        [Fact]
        public void IncorrectTableNameThrowsException()
        {
            var ex = Assert.Throws<StarRocksConfigurationException>(() =>
            {
                new StarRocksPrimaryKeySink(new StarRocksSinkOptions()
                {
                    HttpUrl = "http://",
                    Credentials = () => new StarRocksUsernamePassword("root", "")
                }, Core.Operators.Write.ExecutionMode.OnCheckpoint, _invalidTableNameWriteRel, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());
            });

            Assert.Equal("Starrocks table name must be in the format 'database.table'", ex.Message);
        }

        [Fact]
        public async Task CreateTransactionErrorThrowsException()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default!).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "Cant create transaction",
                Status = StarRocksStatus.Failed
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            var ex = await Assert.ThrowsAsync<StarRocksTransactionException>(async () =>
            {
                await op.Completion;
            });

            Assert.Equal("Cant create transaction", ex.Message);
        }

        /// <summary>
        /// Check that rollback is called after a LABEL_ALREADY_EXISTS error when creating a transaction
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task CreateTransactionLabelAlreadyExistCallsRollback()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "Already exists",
                Status = StarRocksStatus.LabelAlreadyExists
            }));
            client.TransactionRollback(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = StarRocksStatus.Ok,
                Message = "ok"
            }));
            

            var op = await SendOneRowAndSendCheckpoint(client);

            await Assert.ThrowsAsync<StarRocksTransactionException>(async () =>
            {
                await op.Completion;
            });

            await client.ReceivedWithAnyArgs().TransactionRollback(default!);
        }

        [Fact]
        public async Task TransactionLoadFailureCallsRollback()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "ok",
                Status = StarRocksStatus.Ok
            }));
            client.TransactionLoad(default).ThrowsAsyncForAnyArgs(new Exception("Error loading data"));


            var op = await SendOneRowAndSendCheckpoint(client);

            var ex = await Assert.ThrowsAsync<Exception>(async () =>
            {
                await op.Completion;
            });

            Assert.Equal("Error loading data", ex.Message);

            await client.ReceivedWithAnyArgs().TransactionRollback(default);
        }

        [Fact]
        public async Task TransactionPrepareFailureCallsRollback()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "ok",
                Status = StarRocksStatus.Ok
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = StarRocksStatus.Failed,
                Message = "Error"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            var ex = await Assert.ThrowsAsync<StarRocksTransactionException>(async () =>
            {
                await op.Completion;
            });

            Assert.Equal("Error", ex.Message);

            await client.ReceivedWithAnyArgs().TransactionRollback(default);
        }

        [Fact]
        public async Task TransactionCommitFailedThrowsException()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "ok",
                Status = StarRocksStatus.Ok
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = StarRocksStatus.Ok,
                Message = "ok"
            }));
            client.TransactionCommit(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.StreamLoadInfo()
            {
                Status = StarRocksStatus.Failed,
                Message = "Commit failure"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            await EgressWaitForCheckpointDone();

            // Call compact which happens after checkpoints
            var ex = await Assert.ThrowsAsync<StarRocksTransactionException>(async () =>
            {
                await op.Compact();
            });
            Assert.Equal("Transaction commit failed with status 'Failed' and message 'Commit failure'", ex.Message);
        }

        [Fact]
        public async Task TransactionCommitIsCalledOnCompact()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "ok",
                Status = StarRocksStatus.Ok
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = StarRocksStatus.Ok,
                Message = "ok"
            }));
            client.TransactionCommit(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.StreamLoadInfo()
            {
                Status = StarRocksStatus.Ok,
                Message = "ok"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            await EgressWaitForCheckpointDone();


            await op.Compact();

            await client.ReceivedWithAnyArgs().TransactionCommit(default);
        }

        [Fact]
        public async Task TransactionCommitIsCalledOnInitAfterCrash()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.CreateTransaction(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Message = "ok",
                Status = StarRocksStatus.Ok
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = StarRocksStatus.Ok,
                Message = "ok"
            }));
            client.TransactionCommit(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.StreamLoadInfo()
            {
                Status = StarRocksStatus.Ok,
                Message = "ok"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            await EgressWaitForCheckpointDone();

            // Simulate a crash where the operator reinitializes, expected here is that the prepared transaction is committed
            await ReinitializeOperator(op);

            await client.ReceivedWithAnyArgs().TransactionCommit(default);
        }

        private async Task<StarRocksPrimaryKeySink> SendOneRowAndSendCheckpoint(IStarrocksClient client)
        {
            var factory = new StarRocksMockClientFactory(client);

            var op = new ObservedSink(new StarRocksSinkOptions()
            {
                HttpUrl = "http://",
                Credentials = () => new StarRocksUsernamePassword("root", ""),
                ClientFactory = factory
            }, Core.Operators.Write.ExecutionMode.OnCheckpoint, _writeRel);

            await InitializeOperator(op);

            await SendWatermarkInitialize(op, new HashSet<string>() { "table" });

            await SendDataBatchWithWeightOne(op, [new { a = "value1" }]);

            await SendWatermark(op, new Dictionary<string, AbstractWatermarkValue>()
            {
                {"table", LongWatermarkValue.Create(1) }
            });

            // Init event also signals checkpoint done, let it pass first
            await WaitForHandled(op, "watermark");

            await SendCheckpoint(op, 1);

            return op;
        }

        [Fact]
        public async Task NoPrimaryKeyOnTableThrowsException()
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = []
            }));

            var factory = new StarRocksMockClientFactory(client);

            var op = new StarRocksPrimaryKeySink(new StarRocksSinkOptions()
            {
                HttpUrl = "http://",
                Credentials = () => new StarRocksUsernamePassword("root", ""),
                ClientFactory = factory
            }, Core.Operators.Write.ExecutionMode.OnCheckpoint, _writeRel, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());

            var ex = await Assert.ThrowsAsync<StarRocksConfigurationException>(async () =>
            {
                await InitializeOperator(op);
            });
            Assert.Equal("The target Starrocks table does not have a primary key defined.", ex.Message);
        }

        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

        [Fact]
        public async Task WatermarksMergeWhileStreamLoadInFlight()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads);

            await SendRowAndWatermark(op, "value1", 1);
            var first = await NextLoad(loads);
            Assert.Contains("value1", first.Body);

            // Both arrive while the first load is in flight
            await SendRowAndWatermark(op, "value2", 2);
            await SendRowAndWatermark(op, "value3", 3);
            Assert.False(loads.Reader.TryRead(out _));

            first.Done.SetResult();
            var merged = await NextLoad(loads);
            Assert.Contains("value2", merged.Body);
            Assert.Contains("value3", merged.Body);
            Assert.DoesNotContain("value1", merged.Body);
            merged.Done.SetResult();
        }

        [Fact]
        public async Task CheckpointWaitsForAllStreamLoads()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads);

            await SendRowAndWatermark(op, "value1", 1);
            var first = await NextLoad(loads);
            await SendRowAndWatermark(op, "value2", 2);

            await SendCheckpoint(op, 1);
            var checkpointDone = EgressWaitForCheckpointDone();

            first.Done.SetResult();
            // Deferred watermark is sent before the checkpoint completes
            var second = await NextLoad(loads);
            Assert.Contains("value2", second.Body);
            Assert.False(checkpointDone.IsCompleted);

            second.Done.SetResult();
            await checkpointDone.WaitAsync(WaitTimeout);
        }

        [Fact]
        public async Task RowsAfterWatermarkWaitForTheirWatermark()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads);

            await SendRowAndWatermark(op, "value1", 1);
            var first = await NextLoad(loads);
            await SendRowAndWatermark(op, "value2", 2);
            await SendDataBatchWithWeightOne(op, [new { a = "value3" }]);
            await WaitForHandled(op, "rows");

            // Send done mid watermark, nothing flushes yet
            first.Done.SetResult();
            await WaitForHandled(op, "trigger");
            Assert.False(loads.Reader.TryRead(out _));

            await SendWatermark(op, WatermarkValues(3));
            var second = await NextLoad(loads);
            Assert.Contains("value2", second.Body);
            Assert.Contains("value3", second.Body);
            second.Done.SetResult();
        }

        [Fact]
        public async Task FailedBackgroundStreamLoadFailsOperator()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads);

            await SendRowAndWatermark(op, "value1", 1);
            var first = await NextLoad(loads);
            first.Done.SetException(new Exception("Load failed"));

            var ex = await Assert.ThrowsAsync<Exception>(() => op.Completion.WaitAsync(WaitTimeout));
            Assert.Equal("Load failed", ex.Message);
        }

        [Fact]
        public async Task CanceledBackgroundStreamLoadFailsOperator()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads);

            await SendRowAndWatermark(op, "value1", 1);
            var first = await NextLoad(loads);
            // Same as an HttpClient timeout
            first.Done.SetCanceled();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => op.Completion.WaitAsync(WaitTimeout));
            Assert.IsType<TaskCanceledException>(ex.InnerException);
        }

        [Fact]
        public async Task DisposeWaitsForInFlightLoadAndDropsItsFailure()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads);

            await SendRowAndWatermark(op, "value1", 1);
            var first = await NextLoad(loads);

            var dispose = op.DisposeAsync().AsTask();
            Assert.False(dispose.IsCompleted);

            first.Done.SetException(new Exception("Old run failure"));
            await dispose.WaitAsync(WaitTimeout);

            // Late completion signal must not fail a later run
            await WaitForHandled(op, "trigger");
            Assert.False(op.Completion.IsCompleted);
        }

        private sealed record PendingLoad(string Body, TaskCompletionSource Done);

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        public async Task StreamLoadsInFlightNeverExceedMaxParallelLoads(int maxParallelLoads)
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads, batchSize: 1, maxParallelLoads: maxParallelLoads);

            // One row per load, one more than the limit
            var rows = Enumerable.Range(0, maxParallelLoads + 1).Select(i => new { a = $"value{i}" }).ToList();
            await SendDataBatchWithWeightOne(op, rows);
            await SendWatermark(op, WatermarkValues(1));

            var started = new List<PendingLoad>();
            for (int i = 0; i < maxParallelLoads; i++)
            {
                started.Add(await NextLoad(loads));
            }
            // Upload waits for a free slot, nothing else starts
            Assert.False(loads.Reader.TryRead(out _));

            started[0].Done.SetResult();
            var last = await NextLoad(loads);
            await WaitForHandled(op, "watermark");
            foreach (var load in started.Skip(1).Append(last))
            {
                load.Done.SetResult();
            }

            var bodies = started.Append(last).Select(x => x.Body).ToList();
            foreach (var row in rows)
            {
                Assert.Single(bodies, body => body.Contains($"\"{row.a}\""));
            }

            await SendCheckpoint(op, 1);
            await EgressWaitForCheckpointDone().WaitAsync(WaitTimeout);
        }

        [Fact]
        public async Task OutOfOrderCompletionFreesSlotForNextLoad()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads, batchSize: 1, maxParallelLoads: 3);

            await SendDataBatchWithWeightOne(op, Enumerable.Range(0, 4).Select(i => new { a = $"value{i}" }).ToList());
            await SendWatermark(op, WatermarkValues(1));
            var first = await NextLoad(loads);
            var second = await NextLoad(loads);
            var third = await NextLoad(loads);
            Assert.False(loads.Reader.TryRead(out _));

            // Slow first load must not hold the slot
            second.Done.SetResult();
            var fourth = await NextLoad(loads);
            Assert.False(first.Done.Task.IsCompleted);
            await WaitForHandled(op, "watermark");
            Assert.False(loads.Reader.TryRead(out _));

            first.Done.SetResult();
            third.Done.SetResult();
            fourth.Done.SetResult();
            await SendCheckpoint(op, 1);
            await EgressWaitForCheckpointDone().WaitAsync(WaitTimeout);
        }

        [Fact]
        public async Task PartialTailWaitsForSlotAndCheckpointWaitsForTail()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads, batchSize: 2, maxParallelLoads: 2);

            // Two full batches fill both slots, one row left
            await SendDataBatchWithWeightOne(op, Enumerable.Range(0, 5).Select(i => new { a = $"value{i}" }).ToList());
            await SendWatermark(op, WatermarkValues(1));
            var first = await NextLoad(loads);
            var second = await NextLoad(loads);

            // Tail waits in the background, operator is free
            await WaitForHandled(op, "watermark");
            Assert.False(loads.Reader.TryRead(out _));

            await SendCheckpoint(op, 1);
            var checkpointDone = EgressWaitForCheckpointDone();

            second.Done.SetResult();
            var tail = await NextLoad(loads);
            Assert.Contains("\"value4\"", tail.Body);
            Assert.DoesNotContain("\"value3\"", tail.Body);
            Assert.False(checkpointDone.IsCompleted);

            first.Done.SetResult();
            Assert.False(checkpointDone.IsCompleted);
            tail.Done.SetResult();
            await checkpointDone.WaitAsync(WaitTimeout);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task FailedPredecessorFaultsOperatorWithoutStartingTail(bool canceled)
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            // Single predecessor, only its failure can free the slot
            var op = await CreateWatermarkSink(loads, batchSize: 2, maxParallelLoads: 1);

            await SendDataBatchWithWeightOne(op, Enumerable.Range(0, 3).Select(i => new { a = $"value{i}" }).ToList());
            await SendWatermark(op, WatermarkValues(1));
            var first = await NextLoad(loads);
            await WaitForHandled(op, "watermark");
            Assert.False(loads.Reader.TryRead(out _));

            if (canceled)
            {
                first.Done.SetCanceled();
            }
            else
            {
                first.Done.SetException(new Exception("Load failed"));
            }

            // A started tail would never finish, the fault proves it was skipped
            if (canceled)
            {
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => op.Completion.WaitAsync(WaitTimeout));
                Assert.IsType<TaskCanceledException>(ex.InnerException);
            }
            else
            {
                var ex = await Assert.ThrowsAsync<Exception>(() => op.Completion.WaitAsync(WaitTimeout));
                Assert.Equal("Load failed", ex.Message);
            }
            Assert.False(loads.Reader.TryRead(out _));
        }

        [Fact]
        public async Task NextUploadWaitsForAllParallelLoads()
        {
            var loads = Channel.CreateUnbounded<PendingLoad>();
            var op = await CreateWatermarkSink(loads, batchSize: 1, maxParallelLoads: 2);

            await SendDataBatchWithWeightOne(op, [new { a = "value1" }, new { a = "value2" }]);
            await SendWatermark(op, WatermarkValues(1));
            await WaitForHandled(op, "watermark");
            var first = await NextLoad(loads);
            var second = await NextLoad(loads);

            await SendRowAndWatermark(op, "value3", 2);

            // One load still in flight, next upload must wait
            first.Done.SetResult();
            Assert.False(loads.Reader.TryRead(out _));

            second.Done.SetResult();
            var third = await NextLoad(loads);
            Assert.Contains("value3", third.Body);
            third.Done.SetResult();
        }

        [Fact]
        public void MaxParallelLoadsBelowOneThrowsException()
        {
            var ex = Assert.Throws<StarRocksConfigurationException>(() =>
            {
                new StarRocksPrimaryKeySink(new StarRocksSinkOptions()
                {
                    HttpUrl = "http://",
                    Credentials = () => new StarRocksUsernamePassword("root", ""),
                    MaxParallelLoads = 0
                }, ExecutionMode.OnWatermark, _writeRel, new ExecutionDataflowBlockOptions());
            });

            Assert.Equal("MaxParallelLoads must be at least 1.", ex.Message);
        }

        private async Task<ObservedSink> CreateWatermarkSink(Channel<PendingLoad> loads, int batchSize = 1000, int maxParallelLoads = 1)
        {
            var client = Substitute.For<IStarrocksClient>();
            client.GetTableInfo(default!).ReturnsForAnyArgs(Task.FromResult(new TableInfo()
            {
                ColumnNames = ["a"],
                PrimaryKeys = ["a"]
            }));
            client.StreamLoad(default).ReturnsForAnyArgs(call =>
            {
                var info = call.Arg<StarRocksStreamLoadInfo>();
                var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                loads.Writer.TryWrite(new PendingLoad(Encoding.UTF8.GetString(info.data.Span), done));
                return done.Task;
            });

            var op = new ObservedSink(new StarRocksSinkOptions()
            {
                HttpUrl = "http://",
                Credentials = () => new StarRocksUsernamePassword("root", ""),
                ClientFactory = new StarRocksMockClientFactory(client),
                BatchSize = batchSize,
                MaxParallelLoads = maxParallelLoads
            }, ExecutionMode.OnWatermark, _writeRel);

            await InitializeOperator(op);
            await SendWatermarkInitialize(op, new HashSet<string>() { "table" });
            return op;
        }

        private async Task SendRowAndWatermark(ObservedSink op, string value, long watermark)
        {
            await SendDataBatchWithWeightOne(op, [new { a = value }]);
            await SendWatermark(op, WatermarkValues(watermark));
            await WaitForHandled(op, "watermark");
        }

        private static Dictionary<string, AbstractWatermarkValue> WatermarkValues(long value)
        {
            return new Dictionary<string, AbstractWatermarkValue>()
            {
                { "table", LongWatermarkValue.Create(value) }
            };
        }

        private static async Task<PendingLoad> NextLoad(Channel<PendingLoad> loads)
        {
            return await loads.Reader.ReadAsync().AsTask().WaitAsync(WaitTimeout);
        }

        private static async Task WaitForHandled(ObservedSink op, string kind)
        {
            while (await op.Handled.Reader.ReadAsync().AsTask().WaitAsync(WaitTimeout) != kind)
            {
            }
        }

        /// <summary>
        /// Reports each handled event, gives tests a sync point.
        /// </summary>
        private sealed class ObservedSink : StarRocksPrimaryKeySink
        {
            public Channel<string> Handled { get; } = Channel.CreateUnbounded<string>();

            public ObservedSink(StarRocksSinkOptions options, ExecutionMode executionMode, WriteRelation writeRelation)
                : base(options, executionMode, writeRelation, new ExecutionDataflowBlockOptions())
            {
            }

            protected override async Task OnRecieve(StreamEventBatch msg, long time)
            {
                await base.OnRecieve(msg, time);
                Handled.Writer.TryWrite("rows");
            }

            protected override async Task OnWatermark(Watermark watermark)
            {
                await base.OnWatermark(watermark);
                Handled.Writer.TryWrite("watermark");
            }

            public override async Task OnTrigger(string name, object? state)
            {
                await base.OnTrigger(name, state);
                Handled.Writer.TryWrite("trigger");
            }
        }

        private readonly WriteRelation _writeRel = new WriteRelation()
        {
            Input = new ReadRelation()
            {
                BaseSchema = new Substrait.Type.NamedStruct()
                {
                    Names = ["a"]
                },
                NamedTable = new Substrait.Type.NamedTable()
                {
                    Names = ["table"]
                }
            },
            NamedObject = new Substrait.Type.NamedTable()
            {
                Names = ["db", "output"]
            },
            TableSchema = new Substrait.Type.NamedStruct()
            {
                Names = ["a"],
                Struct = new Substrait.Type.Struct()
                {
                    Types = [new StringType()]
                }
            }
        };

        private readonly WriteRelation _invalidTableNameWriteRel = new WriteRelation()
        {
            Input = new ReadRelation()
            {
                BaseSchema = new Substrait.Type.NamedStruct()
                {
                    Names = ["a"]
                },
                NamedTable = new Substrait.Type.NamedTable()
                {
                    Names = ["table"]
                }
            },
            NamedObject = new Substrait.Type.NamedTable()
            {
                Names = ["output"]
            },
            TableSchema = new Substrait.Type.NamedStruct()
            {
                Names = ["a"],
                Struct = new Substrait.Type.Struct()
                {
                    Types = [new StringType()]
                }
            }
        };
    }
}
