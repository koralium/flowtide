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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.SqlServer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    public class SqlServerSourceFactory : AbstractConnectorSourceFactory, IConnectorTableProviderFactory
    {
        private readonly SqlServerSourceOptions _options;
        private readonly SqlServerTableProvider _tableProvider;

        public SqlServerSourceFactory(SqlServerSourceOptions options)
        {
            _options = options;
            _tableProvider = new SqlServerTableProvider(options.ConnectionStringFunc, options.UseDatabaseDefinedInConnectionStringOnly);
        }

        /// <summary>
        /// Loads all available table names in the database
        /// </summary>
        /// <returns></returns>
        private async Task<HashSet<string>> LoadAvailableTablesList()
        {
            using var conn = new SqlConnection(_options.ConnectionStringFunc());
            conn.Open();
            var tableNamesList = await SqlServerUtils.GetFullTableNames(conn);
            return tableNamesList.ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            return _tableProvider.TryGetTableInformation(tableName, out _);
        }

        public ITableProvider Create()
        {
            return _tableProvider;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;

            string fullName = string.Join(".", tableName);

            using var conn = new SqlConnection(_options.ConnectionStringFunc());
            conn.Open();
            var primaryKeys = SqlServerUtils.GetPrimaryKeys(conn, fullName).GetAwaiter().GetResult();

            List<int> pkIndices = new List<int>();
            foreach (var pk in primaryKeys)
            {
                var pkIndex = readRelation.BaseSchema.Names.FindIndex((s) => s.Equals(pk, StringComparison.OrdinalIgnoreCase));
                if (pkIndex == -1)
                {
                    readRelation.BaseSchema.Names.Add(pk);
                    readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                    pkIndices.Add(readRelation.BaseSchema.Names.Count - 1);
                }
                else
                {
                    pkIndices.Add(pkIndex);
                }
            }

            return readRelation;
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            var options = InitializeOperatorOptions(readRelation).GetAwaiter().GetResult();
            if (options.PartitionMetadata != null)
            {
                return new PartitionedTableDataSource(options, readRelation, functionsRegister, dataflowBlockOptions);
            }
            else
            {
                return new ColumnSqlServerBatchDataSource(options, readRelation, functionsRegister, dataflowBlockOptions);
            }
        }

        private async Task<SqlServerSourceOptions> InitializeOperatorOptions(ReadRelation readRelation)
        {
            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;

            string fullName = string.Join(".", tableName);
            using var connection = new SqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync();

            var options = new SqlServerSourceOptions
            {
                ConnectionStringFunc = _options.ConnectionStringFunc,
                TableNameTransform = _options.TableNameTransform,
                UseDatabaseDefinedInConnectionStringOnly = _options.UseDatabaseDefinedInConnectionStringOnly,
                EnableFullReload = _options.EnableFullReload,
                AllowFullReloadOnTablesWithoutChangeTracking = _options.AllowFullReloadOnTablesWithoutChangeTracking,
                FullLoadMaxRowCount = _options.FullLoadMaxRowCount,
                FullReloadInterval = _options.FullReloadInterval,
                ChangeTrackingInterval = _options.ChangeTrackingInterval,
                ResiliencePipeline = _options.ResiliencePipeline,
            };

            var isChangeTrackingEnabled = await SqlServerUtils.IsChangeTrackingEnabled(connection, fullName);

            options.PartitionMetadata = await SqlServerUtils.GetPartitionMetadata(connection, readRelation);

            if (!isChangeTrackingEnabled)
            {
                options.IsChangeTrackingEnabled = false;
                options.IsView = await SqlServerUtils.IsView(connection, fullName);

                if (options.EnableFullReload)
                {
                    if (!options.IsView && !options.AllowFullReloadOnTablesWithoutChangeTracking)
                    {
                        throw new InvalidOperationException($"Change tracking must be enabled on table '{fullName}'. {nameof(options.EnableFullReload)} or {nameof(options.AllowFullReloadOnTablesWithoutChangeTracking)} must be set to true.");
                    }

                    if (!options.FullReloadInterval.HasValue)
                    {
                        throw new InvalidOperationException($"{nameof(_options.FullReloadInterval)} must be set when {nameof(options.EnableFullReload)} is enabled.");
                    }

                    if (options.FullLoadMaxRowCount.HasValue)
                    {
                        var count = SqlServerUtils.GetRowCount(connection, fullName).GetAwaiter().GetResult();
                        var max = options.FullLoadMaxRowCount;
                        if (count < 0 || count > max)
                        {
                            throw new InvalidOperationException($"Row count of view {fullName} is too large, max allowed rows according to {nameof(options.FullLoadMaxRowCount)} is {max:0,0} rows (actual: {count:0,0}).");
                        }
                    }
                }
                else if (options.IsView)
                {
                    throw new InvalidOperationException($"{nameof(options.EnableFullReload)} must be enabled on the source to read from view.");
                }
                else
                {
                    throw new InvalidOperationException($"Change tracking must be enabled on table '{fullName}'");
                }
            }
            else if (!options.ChangeTrackingInterval.HasValue)
            {
                options.ChangeTrackingInterval = TimeSpan.FromSeconds(1);
            }

            return options;
        }
    }
}
