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

            var isChangeTrackingEnabled = SqlServerUtils.IsChangeTrackingEnabled(conn, fullName).GetAwaiter().GetResult();

            _options.PartitionMetadata = SqlServerUtils.GetPartitionMetadata(conn, readRelation).GetAwaiter().GetResult();

            if (!isChangeTrackingEnabled)
            {
                _options.IsChangeTrackingEnabled = false;
                _options.IsView = SqlServerUtils.IsView(conn, fullName).GetAwaiter().GetResult();

                if (_options.EnableFullReload)
                {
                    if (!_options.IsView && !_options.AllowFullReloadOnTablesWithoutChangeTracking)
                    {
                        throw new InvalidOperationException($"Change tracking must be enabled on table '{fullName}'. {nameof(_options.EnableFullReload)} or {nameof(_options.AllowFullReloadOnTablesWithoutChangeTracking)} must be set to true.");
                    }

                    if (!_options.FullReloadInterval.HasValue)
                    {
                        throw new InvalidOperationException($"{nameof(_options.FullReloadInterval)} must be set when {nameof(_options.EnableFullReload)} is enabled.");
                    }

                    if (_options.FullLoadMaxRowCount.HasValue)
                    {
                        var count = SqlServerUtils.GetRowCount(conn, fullName).GetAwaiter().GetResult();
                        var max = _options.FullLoadMaxRowCount;
                        if (count < 0 || count > max)
                        {
                            throw new InvalidOperationException($"Row count of view {fullName} is too large, max allowed rows according to {nameof(_options.FullLoadMaxRowCount)} is {max:0,0} rows (actual: {count:0,0}).");
                        }
                    }
                }
                else if (_options.IsView)
                {
                    throw new InvalidOperationException($"{nameof(_options.EnableFullReload)} must be enabled on the source to read from view.");
                }
                else
                {
                    throw new InvalidOperationException($"Change tracking must be enabled on table '{fullName}'");
                }
            }
            else if (!_options.ChangeTrackingInterval.HasValue)
            {
                _options.ChangeTrackingInterval = TimeSpan.FromSeconds(1);
            }

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
            using var conn = new SqlConnection(_options.ConnectionStringFunc());
            conn.Open();
            var m = SqlServerUtils.GetPartitionMetadata(conn, readRelation).GetAwaiter().GetResult();
            if (m != null)
            {
                return new PartitionedTableDataSource(_options, m, readRelation, functionsRegister, dataflowBlockOptions);
            }
            else
            {
                return new ColumnSqlServerBatchDataSource(_options, readRelation, functionsRegister, dataflowBlockOptions);
            }
        }
    }
}
