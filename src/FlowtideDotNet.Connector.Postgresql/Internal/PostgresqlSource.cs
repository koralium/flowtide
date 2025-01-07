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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    internal class PostgresqlSourceState : ColumnBatchReadState
    {

    }
    internal class PostgresqlSource : ColumnBatchReadBaseOperator<PostgresqlSourceState>
    {
        public PostgresqlSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
        }

        public override string DisplayName => throw new NotImplementedException();

        protected override Task<PostgresqlSourceState> Checkpoint(long checkpointTime)
        {
            throw new NotImplementedException();
        }

        //protected override Task DeltaLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        //{
        //    return base.DeltaLoadTrigger(output, state);
        //}

        protected override async Task InitializeOrRestore(long restoreTime, PostgresqlSourceState? state, IStateManagerClient stateManagerClient)
        {
            NpgsqlDataSource npgsqlDataSource = NpgsqlDataSource.Create("");
            using var listPublicationsCommand = npgsqlDataSource.CreateCommand("select * from pg_publication_tables");
            using var reader = await listPublicationsCommand.ExecuteReaderAsync();

            var pubNameOrdinal = reader.GetOrdinal("pubname");
            var pubTableNameOrdinal = reader.GetOrdinal("tablename");
            var schemaNameOrdinal = reader.GetOrdinal("schemaname");

            while (await reader.ReadAsync())
            {
                var publicationName = reader.GetString(pubNameOrdinal);
                var tableName = reader.GetString(pubTableNameOrdinal);
                var schemaName = reader.GetString(schemaNameOrdinal);
            }
            await base.InitializeOrRestore(restoreTime, state, stateManagerClient);
        }

        protected override IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default)
        {
            throw new NotImplementedException();
        }

        protected override IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default)
        {
            throw new NotImplementedException();
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            throw new NotImplementedException();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            throw new NotImplementedException();
        }
    }
}
