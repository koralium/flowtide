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

using FlowtideDotNet.Storage.Queue;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    /// <summary>
    /// This class handles reading replication details from a publication.
    /// It allows reading replication details from multiple tables and notificies all subscribers of the changes.
    /// </summary>
    internal class PostgresReplicationHandler
    {
        private enum StateType
        {
            NotStarted,
            Subscribing,
            Running,
            Stopped
        }

        private IAsyncEnumerator<PgOutputReplicationMessage>? enumerator;
        private string replicationSlotName;
        private TaskCompletionSource _barrierSource;
        private int _sourceCount;
        private int _subscribeCount;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly PostgresSourceOptions _options;
        private CancellationTokenSource _cancellationTokenSource;
        private StateType _state;
        private Task? _runTask;
        //private readonly List<string> _publications;
        private readonly Dictionary<string, HashSet<string>> _publicationToTables;
        private Dictionary<uint, string>? _dataTypeIdToType;

        public PostgresReplicationHandler(PostgresSourceOptions options)
        {
            _state = StateType.NotStarted;
            replicationSlotName = $"repl{Guid.NewGuid().ToString().Replace("-", "")}";
            _barrierSource = new TaskCompletionSource();
            _cancellationTokenSource = new CancellationTokenSource();
            this._options = options;
            _publicationToTables = new Dictionary<string, HashSet<string>>();
        }

        public void AddPostgresSourceCount()
        {
            _lock.Wait();
            try
            {
                _sourceCount++;
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Subscribe(
            string pubName, 
            string tableName, 
            ulong walLocation,
            Func<BeginMessage, ValueTask> beginMessageFunc,
            Func<RelationMessage, ValueTask> newRelationFunc,
            Func<InsertMessage, ValueTask> insertMessageFunc,
            Func<UpdateMessage, ValueTask> updateMessageFunc,
            Func<DeleteMessage, ValueTask> deleteMessageFunc,
            Func<CommitMessage, ValueTask> commitMessageFunc)
        {
            await _lock.WaitAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
            try
            {
                if (_state == StateType.Running)
                {
                    // This code happens in case of an error where the stream restarts, then we subscribe during running
                    _cancellationTokenSource.Cancel();
                    // Wait for replication task to cancel
                    if (_runTask != null)
                    {
                        await _runTask.ConfigureAwait(false);
                        _runTask = null;
                    }

                    _subscribeCount = 0;
                    _state = StateType.NotStarted;
                    _publicationToTables.Clear();
                    _barrierSource = new TaskCompletionSource();
                    _cancellationTokenSource = new CancellationTokenSource();
                }
                _subscribeCount++;

                
                if (!_publicationToTables.TryGetValue(pubName, out var tableSet))
                {
                    tableSet = new HashSet<string>();
                    _publicationToTables.Add(pubName, tableSet);
                }
                tableSet.Add(tableName);
                if (_subscribeCount == _sourceCount)
                {
                    // If all sources are subscribed, we can start the replication
                    using var conn = new NpgsqlConnection(_options.ConnectionStringFunc());
                    await conn.OpenAsync();

                    _dataTypeIdToType = await PostgresClientUtils.GetDataTypeIdToTypeList(conn);

                    foreach (var kv in _publicationToTables)
                    {
                        var publicationexist = await PostgresClientUtils.CheckIfPublicationExists(conn, kv.Key);
                        await PostgresClientUtils.CreatePublication(conn, kv.Key, kv.Value.ToList());
                    }
                    

                    _runTask = Task.Factory.StartNew(RunReplication);
                    _barrierSource.SetResult();


                }
            }
            finally
            {
                _lock.Release();
            }
            await _barrierSource.Task.WaitAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
        }

        private async Task RunReplication()
        {
            try
            {
                await using var conn = new LogicalReplicationConnection(_options.ConnectionStringFunc());
                await conn.Open();
                var replicationSlot = await conn.CreatePgOutputReplicationSlot(replicationSlotName, true);
                enumerator = conn.StartReplication(replicationSlot, new PgOutputReplicationOptions(_publicationToTables.Keys.ToList(), PgOutputProtocolVersion.V1), default, new NpgsqlTypes.NpgsqlLogSequenceNumber(1)).GetAsyncEnumerator();

                while (await enumerator.MoveNextAsync())
                {
                    var msg = enumerator.Current;

                    if (msg is InsertMessage insertMessage)
                    {
                        //insertMessage.NewRow.
                        await foreach(var col in insertMessage.NewRow)
                        {
                            
                            var colValue = await col.Get();
                        }
                    }
                    else if (msg is UpdateMessage updateMessage)
                    {
                        //updateMessage.Relation.RelationId
                        // The row data can only be consumed once
                        await foreach (var col in updateMessage.NewRow)
                        {
                            var colValue = await col.Get();
                        }
                        await foreach (var col in updateMessage.NewRow)
                        {
                            var colValue = await col.Get();
                        }
                    }
                    else if (msg is RelationMessage relationMessage)
                    {
                        foreach(var column in relationMessage.Columns)
                        {
                            
                        }
                    }
                    
                }
            }
            catch(Exception e)
            {

            }
            
        }
    }
}
