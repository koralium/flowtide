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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using Microsoft.Data.SqlClient;
using Polly;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{

    internal class ColumnSqlServerBatchDataSource : ColumnSqlDeltaSource
    {
        public ColumnSqlServerBatchDataSource(SqlServerSourceOptions sourceOptions, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(sourceOptions, readRelation, functionsRegister, options)
        {
            FullLoadInterval = sourceOptions.FullReloadInterval;
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(State != null);
            Debug.Assert(State.Value?.ChangeTrackingVersion != null);
            Debug.Assert(PrimaryKeys != null);
            Debug.Assert(PrimaryKeyOrdinals != null);
            Debug.Assert(ConvertFunctions != null);

            Logger.SelectingAllData(FullTableName, StreamName, Name);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var primaryKeyValues = new Dictionary<string, object>();

            if (Options.IsChangeTrackingEnabled && State.Value.ChangeTrackingVersion < 1)
            {
                using var connection = new SqlConnection(Options.ConnectionStringFunc());
                await connection.OpenAsync(linkedCancellation.Token);
                
                State.Value.ChangeTrackingVersion = await SqlServerUtils.GetLatestChangeVersion(connection, ReadRelation.NamedTable.Names);
            }
            else if (!Options.IsChangeTrackingEnabled)
            {
                // when reading from a view or a table without change tracking, use the server timestamp as the version identifier
                using var connection = new SqlConnection(Options.ConnectionStringFunc());
                await connection.OpenAsync(linkedCancellation.Token);
                State.Value.ChangeTrackingVersion = await SqlServerUtils.GetServerTimestamp(connection);
            }

            var batchSize = 10000;
            while (true)
            {
                linkedCancellation.Token.ThrowIfCancellationRequested();

                var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
                var resilienceState = new FullLoadResilienceState(
                    State,
                    Options,
                    ReadRelation,
                    PrimaryKeys,
                    primaryKeyValues.Count > 0,
                    batchSize,
                    Filter,
                    primaryKeyValues);

                var pipelineResult = await Options.ResiliencePipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
                {
                    Debug.Assert(state?.State.Value?.ChangeTrackingVersion != null);

                    try
                    {
                        var connection = new SqlConnection(state.Options.ConnectionStringFunc());
                        await connection.OpenAsync();

                        var command = connection.CreateCommand();
                        command.CommandText = SqlServerUtils.CreateInitialSelectStatement(state.ReadRelation, state.PrimaryKeys, state.BatchSize, state.IncludePkParameters, state.Filter);

                        foreach (var pk in state.PrimaryKeyValues)
                        {
                            command.Parameters.AddWithValue($"@{pk.Key}", pk.Value);
                        }

                        var reader = await command.ExecuteReaderAsync(ctx.CancellationToken);

                        return Outcome.FromResult(new ResilienceResult(reader, connection, command));
                    }
                    catch (Exception ex)
                    {
                        return Outcome.FromException<ResilienceResult>(ex);
                    }

                }, context, resilienceState);

                ResilienceContextPool.Shared.Return(context);

                pipelineResult.ThrowIfException();
                Debug.Assert(pipelineResult.Result != null);
                var reader = pipelineResult.Result.Reader;

                int elementCount = 0;

                while (await reader.ReadAsync(linkedCancellation.Token))
                {
                    elementCount++;
                    weights.Add(1);
                    iterations.Add(0);

                    primaryKeyValues.Clear();
                    for (int i = 0; i < PrimaryKeyOrdinals.Count; i++)
                    {
                        primaryKeyValues.Add(PrimaryKeys[i], reader.GetValue(PrimaryKeyOrdinals[i]));
                    }

                    for (int i = 0; i < columns.Length; i++)
                    {
                        ConvertFunctions[i](reader, columns[i]);
                    }

                    if (weights.Count >= 100)
                    {
                        var eventBatchData = new EventBatchData(columns);
                        var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                        yield return new ColumnReadEvent(weightedBatch, State.Value.ChangeTrackingVersion);
                        InitializeBatchCollections(out weights, out iterations, out columns);
                    }
                }

                await pipelineResult.Result.DisposeAsync();

                if (elementCount != batchSize)
                {
                    break;
                }
            }

            if (weights.Count > 0)
            {
                var eventBatchData = new EventBatchData(columns);
                var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                yield return new ColumnReadEvent(weightedBatch, State.Value.ChangeTrackingVersion);
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
            }

            linkedCancellation.Dispose();
        }

        private sealed record FullLoadResilienceState(
            IObjectState<SqlServerState> State,
            SqlServerSourceOptions Options,
            ReadRelation ReadRelation,
            List<string> PrimaryKeys,
            bool IncludePkParameters,
            int BatchSize,
            string? Filter,
            Dictionary<string, object> PrimaryKeyValues)
        {

        }
    }
}