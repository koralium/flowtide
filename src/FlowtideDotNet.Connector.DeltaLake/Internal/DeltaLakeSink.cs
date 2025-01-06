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

using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using DeltaLake.Errors;
using DeltaLake.Interfaces;
using DeltaLake.Table;
using FlowtideDotNet.Base;
using FlowtideDotNet.Connector.DeltaLake.Internal.ArrowEncoding;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakeSinkState : ColumnWriteState
    {

    }
    internal class DeltaLakeSink : ColumnGroupedWriteOperator<DeltaLakeSinkState>
    {
        private readonly DeltaLakeSinkOptions m_deltaLakeSinkOptions;
        private readonly WriteRelation m_writeRelation;
        private readonly string m_fullLoadMergeStatement;
        private readonly string m_upsertMergeStatement;
        private readonly IReadOnlyList<int> m_primaryKeys;
        private DeltaEngine? m_engine;
        private ITable? m_table;
        private Schema? m_schema;
        private Schema? m_upsertSchema;
        private RecordBatchEncoder? m_encoder;

        public DeltaLakeSink(
            DeltaLakeSinkOptions deltaLakeSinkOptions,
            ExecutionMode executionMode, 
            WriteRelation writeRelation, 
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            this.m_deltaLakeSinkOptions = deltaLakeSinkOptions;
            this.m_writeRelation = writeRelation;
            m_fullLoadMergeStatement = DeltaLakeUtils.CreateFullLoadMergeIntoStatement(writeRelation, deltaLakeSinkOptions.PrimaryKeyColumns);
            m_upsertMergeStatement = DeltaLakeUtils.CreateUpsertMergeIntoStatement(writeRelation, deltaLakeSinkOptions.PrimaryKeyColumns);
            if (deltaLakeSinkOptions.Table != null)
            {
                m_table = deltaLakeSinkOptions.Table;
                SetSchema(m_table.Schema());
            }
            else if (deltaLakeSinkOptions.TableLocation == null)
            {
                throw new InvalidOperationException("Table or TableLocation must be set in DeltaLakeSinkOptions");
            }
            List<int> pkIndices = new List<int>();
            for (int i = 0; i < deltaLakeSinkOptions.PrimaryKeyColumns.Count; i++)
            {
                var pkIndex = writeRelation.TableSchema.Names.IndexOf(deltaLakeSinkOptions.PrimaryKeyColumns[i]);
                if (pkIndex < 0)
                {
                    throw new InvalidOperationException($"Primary key column {deltaLakeSinkOptions.PrimaryKeyColumns[i]} not found in the insert statement");
                }
                pkIndices.Add(pkIndex);
            }
            m_primaryKeys = pkIndices;
        }

        private void SetSchema(Schema schema)
        {
            m_schema = schema;
            var upsertSchemaBuilder = new Schema.Builder();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                upsertSchemaBuilder.Field(schema.GetFieldByIndex(i));
            }
            upsertSchemaBuilder.Field(new Field("_flowtide_deleted", BooleanType.Default, true));
            m_upsertSchema = upsertSchemaBuilder.Build();
            m_encoder = RecordBatchEncoder.Create(m_schema);
        }

        public override string DisplayName => "DeltaLake";

        protected override DeltaLakeSinkState Checkpoint(long checkpointTime)
        {
            return new DeltaLakeSinkState();
        }

        protected override async Task InitializeOrRestore(long restoreTime, DeltaLakeSinkState? state, IStateManagerClient stateManagerClient)
        {
            if (m_deltaLakeSinkOptions.DeltaEngine != null)
            {
                m_engine = m_deltaLakeSinkOptions.DeltaEngine;
            }
            else
            {
                m_engine = new DeltaEngine(EngineOptions.Default);
            }
            
            try
            {
                if (m_table == null)
                {
                    Debug.Assert(m_deltaLakeSinkOptions.TableLocation != null);
                    m_table = await m_engine.LoadTableAsync(new TableOptions()
                    {
                        TableLocation = m_deltaLakeSinkOptions.TableLocation,
                    }, CancellationToken);
                    SetSchema(m_table.Schema());
                }   
            }
            catch (DeltaRuntimeException e)
            {
                if (!e.Message.Contains("Not a Delta table"))
                {
                    throw;
                }
            }
             
            await base.InitializeOrRestore(restoreTime, state, stateManagerClient);
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return new ValueTask<IReadOnlyList<int>>(m_primaryKeys);
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(m_engine != null);

            var enumerator = rows.GetAsyncEnumerator(cancellationToken);

            if (await enumerator.MoveNextAsync())
            {
                if (m_table == null)
                {
                    Debug.Assert(m_deltaLakeSinkOptions.TableLocation != null);
                    var schema = SchemaFromBatchUtil.GetSchema(m_writeRelation.TableSchema.Names, enumerator.Current.EventBatchData);
                    SetSchema(schema);
                    m_table = await m_engine.CreateTableAsync(new TableCreateOptions(m_deltaLakeSinkOptions.TableLocation, schema)
                    {
                        SaveMode = SaveMode.Append
                    }, CancellationToken);
                }
            
                if (!HasSentInitialData)
                {
                    if (m_table != null)
                    {
                        Debug.Assert(m_schema != null);
                        Debug.Assert(m_encoder != null);
                        await m_table.MergeAsync(m_fullLoadMergeStatement, new BatchCollection(enumerator.Current, enumerator, m_encoder, NativeMemoryAllocator.Default.Value, false), m_schema, default);
                    }
                }
                else
                {
                    if (m_table != null)
                    {
                        Debug.Assert(m_upsertSchema != null);
                        Debug.Assert(m_encoder != null);
                        await m_table.MergeAsync(m_upsertMergeStatement, new BatchCollection(enumerator.Current, enumerator, m_encoder, NativeMemoryAllocator.Default.Value, true), m_upsertSchema, default);
                    }
                }
            }

        }
    }
}
