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
using FlowtideDotNet.Connector.Starrocks.Internal;
using FlowtideDotNet.Core.Tests;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace FlowtideDotNet.Connector.Starrocks.Tests
{
    public class OperatorTests : OperatorTestBase
    {
        [Fact]
        public void IncorrectTableNameThrowsException()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                new StarrocksPrimaryKeySink(new StarrocksSinkOptions()
                {
                    HttpUrl = "http://",
                    Username = "user"
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
                Status = "Fail"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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
                Status = "LABEL_ALREADY_EXISTS"
            }));
            client.TransactionRollback(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = "OK",
                Message = "ok"
            }));
            

            var op = await SendOneRowAndSendCheckpoint(client);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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
                Status = "OK"
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
                Status = "OK"
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = "Fail",
                Message = "Error"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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
                Status = "OK"
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = "OK",
                Message = "ok"
            }));
            client.TransactionCommit(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.StreamLoadInfo()
            {
                Status = "Fail",
                Message = "Commit failure"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            await EgressWaitForCheckpointDone();

            // Call compact which happens after checkpoints
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await op.Compact();
            });
            Assert.Equal("Transaction commit failed with status 'Fail' and message 'Commit failure'", ex.Message);
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
                Status = "OK"
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = "OK",
                Message = "ok"
            }));
            client.TransactionCommit(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.StreamLoadInfo()
            {
                Status = "OK",
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
                Status = "OK"
            }));
            client.TransactionLoad(default).ReturnsForAnyArgs(Task.CompletedTask);
            client.TransactionPrepare(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.TransactionInfo()
            {
                Status = "OK",
                Message = "ok"
            }));
            client.TransactionCommit(default).ReturnsForAnyArgs(Task.FromResult(new Internal.HttpApi.StreamLoadInfo()
            {
                Status = "OK",
                Message = "ok"
            }));

            var op = await SendOneRowAndSendCheckpoint(client);

            await EgressWaitForCheckpointDone();

            // Simulate a crash where the operator reinitializes, expected here is that the prepared transaction is committed
            await ReinitializeOperator(op);

            await client.ReceivedWithAnyArgs().TransactionCommit(default);
        }

        private async Task<StarrocksPrimaryKeySink> SendOneRowAndSendCheckpoint(IStarrocksClient client)
        {
            var factory = new StarrocksMockClientFactory(client);

            var op = new StarrocksPrimaryKeySink(new StarrocksSinkOptions()
            {
                HttpUrl = "http://",
                Username = "user",
                ClientFactory = factory
            }, Core.Operators.Write.ExecutionMode.OnCheckpoint, _writeRel, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());

            await InitializeOperator(op);

            await SendWatermarkInitialize(op, new HashSet<string>() { "table" });

            await SendDataBatchWithWeightOne(op, [new { a = "value1" }]);

            await SendWatermark(op, new Dictionary<string, AbstractWatermarkValue>()
            {
                {"table", LongWatermarkValue.Create(1) }
            });

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

            var factory = new StarrocksMockClientFactory(client);

            var op = new StarrocksPrimaryKeySink(new StarrocksSinkOptions()
            {
                HttpUrl = "http://",
                Username = "user",
                ClientFactory = factory
            }, Core.Operators.Write.ExecutionMode.OnCheckpoint, _writeRel, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await InitializeOperator(op);
            });
            Assert.Equal("The target Starrocks table does not have a primary key defined.", ex.Message);
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
