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
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.SqlServer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    public class SqlServerSourceFactory : AbstractConnectorSourceFactory, IConnectorTableProviderFactory
    {
        private readonly Func<string> _connectionStringFunc;
        private readonly Func<ReadRelation, string>? customTableNameFunc;
        private readonly SqlServerTableProvider _tableProvider;

        public SqlServerSourceFactory(
            Func<string> connectionStringFunc, 
            Func<ReadRelation, string>? tableNameTransform = null,
            bool useDatabaseDefinedInConnectionStringOnly = false)
        {
            this._connectionStringFunc = connectionStringFunc;
            this.customTableNameFunc = tableNameTransform;
            _tableProvider = new SqlServerTableProvider(connectionStringFunc, useDatabaseDefinedInConnectionStringOnly);
        }

        /// <summary>
        /// Loads all available table names in the database
        /// </summary>
        /// <returns></returns>
        private async Task<HashSet<string>> LoadAvailableTablesList()
        {
            using var conn = new SqlConnection(_connectionStringFunc());
            conn.Open();
            var tableNamesList = await SqlServerUtils.GetFullTableNames(conn);
            return tableNamesList.ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            var tableName = customTableNameFunc?.Invoke(readRelation) ?? readRelation.NamedTable.DotSeperated;

            return _tableProvider.TryGetTableInformation(tableName, out _);
        }

        public ITableProvider Create()
        {
            return _tableProvider;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            var tableName = customTableNameFunc?.Invoke(readRelation) ?? readRelation.NamedTable.DotSeperated;

            using var conn = new SqlConnection(_connectionStringFunc());
            conn.Open();
            var primaryKeys = SqlServerUtils.GetPrimaryKeys(conn, tableName).GetAwaiter().GetResult();

            var isChangeTrackingEnabled = SqlServerUtils.IsChangeTrackingEnabled(conn, tableName).GetAwaiter().GetResult();
            if (!isChangeTrackingEnabled)
            {
                throw new InvalidOperationException($"Change tracking must be enabled on table '{tableName}'");
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

            return new NormalizationRelation()
            {
                Input = readRelation,
                Filter = readRelation.Filter,
                KeyIndex = pkIndices,
                Emit = readRelation.Emit
            };
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            var tableName = customTableNameFunc?.Invoke(readRelation) ?? readRelation.NamedTable.DotSeperated;
            return new ColumnSqlServerDataSource(_connectionStringFunc, tableName, readRelation, dataflowBlockOptions);
        }
    }
}
