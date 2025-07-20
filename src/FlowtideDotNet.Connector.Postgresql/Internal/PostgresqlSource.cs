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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Npgsql;
using Npgsql.Replication.PgOutput.Messages;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    internal class PostgresqlSource : ColumnBatchReadBaseOperator
    {
        private readonly PostgresSourceOptions _postgresOptions;
        private readonly PostgresReplicationHandler replicationHandler;
        private readonly ReadRelation _readRelation;
        private List<int>? _primaryKeyIndices;
        private List<Action<NpgsqlDataReader, IColumn>>? _convertFunctions;
        private IReadOnlyList<string>? _primaryKeys;

        public PostgresqlSource(
            PostgresSourceOptions postgresOptions,
            PostgresReplicationHandler replicationHandler,
            ReadRelation readRelation, 
            IFunctionsRegister functionsRegister, 
            DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            this._postgresOptions = postgresOptions;
            this.replicationHandler = replicationHandler;
            this._readRelation = readRelation;
        }

        public override string DisplayName => "Postgres";

        protected override Task Checkpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            using var conn = new NpgsqlConnection(_postgresOptions.ConnectionStringFunc());
            await conn.OpenAsync();

            var replicationColumns = await PostgresClientUtils.GetReplicationColumns(conn, _readRelation.NamedTable.Names);
            _primaryKeys = replicationColumns;

            // Check that all replication columns are in the read relation, if not add them
            List<int> primaryKeyIndices = new List<int>();
            for (int i = 0; i < replicationColumns.Count; i++)
            {
                bool found = false;
                for (int k = 0; k < _readRelation.BaseSchema.Names.Count; k++)
                {
                    if (replicationColumns[i].Equals(_readRelation.BaseSchema.Names[k], StringComparison.OrdinalIgnoreCase))
                    {
                        found = true;
                        primaryKeyIndices.Add(k);
                        break;
                    }
                }
                if (!found)
                {
                    primaryKeyIndices.Add(_readRelation.BaseSchema.Names.Count);
                    _readRelation.BaseSchema.Names.Add(replicationColumns[i]);
                }
            }

            _primaryKeyIndices = primaryKeyIndices;

            // Fetch the schema of the table and also create the convert functions into arrow format
            var schema = await PostgresClientUtils.GetTableSchema(conn, _readRelation);
            _convertFunctions = PostgresClientUtils.GetColumnEventCreator(schema);

            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }


        protected override IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default)
        {
            throw new NotImplementedException();
        }

        private ValueTask OnBeginMessage(BeginMessage beginMessage)
        {
            return ValueTask.CompletedTask;
        }

        private ValueTask OnRelationMessage(RelationMessage relationMessage)
        {
            return ValueTask.CompletedTask;
        }

        private ValueTask OnInsertMessage(InsertMessage insertMessage)
        {
            return ValueTask.CompletedTask;
        }

        private ValueTask OnUpdateMessage(UpdateMessage updateMessage)
        {
            return ValueTask.CompletedTask;
        }

        private ValueTask OnDeleteMessage(DeleteMessage deleteMessage)
        {
            return ValueTask.CompletedTask;
        }

        private ValueTask OnCommitMessage(CommitMessage commitMessage)
        {
            return ValueTask.CompletedTask;
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_primaryKeys != null);
            Debug.Assert(_convertFunctions != null);

            using var conn = new NpgsqlConnection(_postgresOptions.ConnectionStringFunc());
            await conn.OpenAsync();

            // Get the current WAL location which will be used in the subscribe
            var currentWalLocation = await PostgresClientUtils.GetCurrentWalLocation(conn);

            await replicationHandler.Subscribe(
                _postgresOptions.GetPublicationNameFunc(_readRelation), 
                _readRelation.NamedTable.DotSeperated, 
                (ulong)currentWalLocation,
                OnBeginMessage,
                OnRelationMessage,
                OnInsertMessage,
                OnUpdateMessage,
                OnDeleteMessage,
                OnCommitMessage
                );


            int batchSize = 10000;

            IColumn[] outputColumns = new IColumn[_convertFunctions.Count];
            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            for (int i = 0; i < _convertFunctions.Count; i++)
            {
                outputColumns[i] = ColumnFactory.Get(MemoryAllocator);
            }

            Dictionary<string, object> primaryKeyValues = new Dictionary<string, object>();

            List<EventBatchWeighted> weightedBatches = new List<EventBatchWeighted>();

            while (true)
            {
                using var cmd = conn.CreateCommand();

                if (primaryKeyValues.Count == 0)
                {
                    cmd.CommandText = PostgresClientUtils.CreateInitialSelectStatement(_readRelation, _primaryKeys, batchSize, false, null);
                }
                else
                {
                    cmd.CommandText = PostgresClientUtils.CreateInitialSelectStatement(_readRelation, _primaryKeys, batchSize, true, null);
                    foreach (var pk in primaryKeyValues)
                    {
                        cmd.Parameters.Add(new NpgsqlParameter(pk.Key, pk.Value));
                    }
                }

                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                List<int> primaryKeyOrdinals = new List<int>();
                foreach (var pk in _primaryKeys)
                {
                    primaryKeyOrdinals.Add(reader.GetOrdinal(pk));
                }

                int elementCount = 0;
                while (await reader.ReadAsync())
                {
                    elementCount++;
                    weights.Add(1);
                    iterations.Add(0);
                    for (int i = 0; i < _convertFunctions.Count; i++)
                    {
                        _convertFunctions[i](reader, outputColumns[i]);
                    }

                    primaryKeyValues.Clear();
                    for (int i = 0; i < primaryKeyOrdinals.Count; i++)
                    {
                        primaryKeyValues.Add(_primaryKeys[i], reader.GetValue(primaryKeyOrdinals[i]));
                    }

                    if (weights.Count >= 100)
                    {
                        weightedBatches.Add(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                        weights = new PrimitiveList<int>(MemoryAllocator);
                        iterations = new PrimitiveList<uint>(MemoryAllocator);
                        outputColumns = new IColumn[_convertFunctions.Count];
                        for (int i = 0; i < _convertFunctions.Count; i++)
                        {
                            outputColumns[i] = ColumnFactory.Get(MemoryAllocator);
                        }
                    }
                }

                if (weights.Count > 0)
                {
                    weightedBatches.Add(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                    outputColumns = new IColumn[_convertFunctions.Count];
                    for (int i = 0; i < _convertFunctions.Count; i++)
                    {
                        outputColumns[i] = ColumnFactory.Get(MemoryAllocator);
                    }
                }

                foreach (var weightedBatch in weightedBatches)
                {
                    yield return new ColumnReadEvent(weightedBatch, 0);
                }
                weightedBatches.Clear();

                if (elementCount != batchSize)
                {
                    break;
                }
            }
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            Debug.Assert(_primaryKeyIndices != null);
            return ValueTask.FromResult(_primaryKeyIndices);
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _readRelation.NamedTable.DotSeperated });
        }
    }
}
