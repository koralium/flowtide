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

using FlowtideDotNet.Connector.SqlServer.SqlServer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using System.Data;

namespace FlowtideDotNet.SqlServer.Tests
{
    /// <summary>
    /// Most sink tests are done in acceptance tests, tests simple functions in sink
    /// </summary>
    public class SinkTests
    {
        private static WriteRelation CreateWriteRelation()
        {
            return new WriteRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new Substrait.Type.NamedTable()
                    {
                        Names = new List<string>() { "table1" }
                    },
                    BaseSchema = new Substrait.Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" }
                    }
                },
                NamedObject = new Substrait.Type.NamedTable()
                {
                    Names = new List<string>() { "table2" }
                },
                TableSchema = new Substrait.Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" }
                }
            };
        }

        [Fact]
        public void TempTableIsTemporary()
        {
            var sink = new ColumnSqlServerSink(new Connector.SqlServer.SqlServerSinkOptions() { ConnectionStringFunc = () => "" }, CreateWriteRelation(), new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());
            var tableName = sink.GetTmpTableName();
            Assert.StartsWith("#", tableName);
        }

        /// <summary>
        /// Compact attempts the commit hook once per committed version.
        /// </summary>
        [Fact]
        public async Task CompactAttemptsTheCommitHookOncePerCommittedVersion()
        {
            var sink = new ColumnSqlServerSink(new Connector.SqlServer.SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => "",
                CustomBulkCopyDestinationTable = _ => "staging",
                OnCheckpointComplete = (_, _, _, _) => ValueTask.CompletedTask
            }, CreateWriteRelation(), new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions());

            await sink.CheckpointDone(7);
            // Empty connection string fails the attempt, the version is spent.
            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.Compact());
            // Nothing left to commit, no connection is opened.
            await sink.Compact();
        }

        /// <summary>
        /// Commit hook without a staging table is refused.
        /// </summary>
        [Fact]
        public void CommitHookWithoutAStagingTableIsRefused()
        {
            var e = Assert.Throws<InvalidOperationException>(() => new ColumnSqlServerSink(new Connector.SqlServer.SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => "",
                OnCheckpointComplete = (_, _, _, _) => ValueTask.CompletedTask
            }, CreateWriteRelation(), new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()));

            Assert.Contains("CustomBulkCopyDestinationTable", e.Message);
        }

        [Fact]
        public void PrimaryKeyColumnsResolveToTheCasingOfTheDestinationTable()
        {
            var (indices, columnNames) = SqlServerUtils.ResolvePrimaryKeyColumns(
                new List<string>() { "guid-dash", "Name" },
                new List<string>() { "NAME" },
                "test-db.dbo.dest");

            Assert.Equal(new List<int>() { 1 }, indices);
            Assert.Equal(new List<string>() { "Name" }, columnNames);
        }

        [Fact]
        public void PrimaryKeyColumnThatIsNotWrittenThrows()
        {
            var e = Assert.Throws<InvalidOperationException>(() =>
            {
                SqlServerUtils.ResolvePrimaryKeyColumns(
                    new List<string>() { "guid-dash", "Name" },
                    new List<string>() { "id" },
                    "test-db.dbo.dest");
            });

            Assert.Equal("All primary keys of the sink table must be sent to the sink operator, 'id' is not written to 'test-db.dbo.dest'.", e.Message);
        }

        /// <summary>
        /// The merge statement must never update a primary key column.
        /// </summary>
        [Fact]
        public void MergeStatementDoesNotUpdatePrimaryKeyDeclaredWithOtherCasing()
        {
            var writtenColumnNames = new List<string>() { "Name", "guid-dash" };

            using var dataTable = new DataTable();
            dataTable.Columns.Add("md_operation");
            foreach (var columnName in writtenColumnNames)
            {
                dataTable.Columns.Add(columnName);
            }

            var (_, primaryKeyColumnNames) = SqlServerUtils.ResolvePrimaryKeyColumns(
                writtenColumnNames,
                new List<string>() { "name" },
                "dest");

            var statement = SqlServerUtils.CreateMergeIntoProcedure("#tmp", "[dest]", primaryKeyColumnNames.ToHashSet(), dataTable);

            Assert.Contains("UPDATE SET tgt.[guid-dash] = src.[guid-dash]", statement);
            Assert.DoesNotContain("tgt.[Name] = src.[Name]", statement);
        }
    }
}
