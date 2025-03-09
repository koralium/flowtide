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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.SqlServer;
using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    internal class SqlServerSinkFactory : AbstractConnectorSinkFactory
    {
        private readonly SqlServerSinkOptions sqlServerSinkOptions;
        private readonly SqlServerTableProvider _sqlServerTableProvider;

        public SqlServerSinkFactory(SqlServerSinkOptions sqlServerSinkOptions)
        {
            this.sqlServerSinkOptions = sqlServerSinkOptions;
            _sqlServerTableProvider = new SqlServerTableProvider(sqlServerSinkOptions.ConnectionStringFunc, sqlServerSinkOptions.UseDatabaseDefinedInConnectionStringOnly);
        }

        /// <summary>
        /// Loads all available table names in the database
        /// </summary>
        /// <returns></returns>
        private async Task<HashSet<string>> LoadAvailableTablesList()
        {
            using var conn = new SqlConnection(sqlServerSinkOptions.ConnectionStringFunc());
            conn.Open();
            var tableNamesList = await SqlServerUtils.GetFullTableNames(conn);
            return tableNamesList.ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        public override bool CanHandle(WriteRelation writeRelation)
        {
            return _sqlServerTableProvider.TryGetTableInformation(writeRelation.NamedObject.Names, out _);
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new ColumnSqlServerSink(sqlServerSinkOptions, writeRelation, dataflowBlockOptions);
        }
    }
}
