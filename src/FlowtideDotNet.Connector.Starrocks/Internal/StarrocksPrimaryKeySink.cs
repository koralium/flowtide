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

using FlowtideDotNet.Base;
using FlowtideDotNet.Connector.Starrocks.Exceptions;
using FlowtideDotNet.Connector.Starrocks.Internal.HttpApi;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.StarRocks.Internal
{
    internal class StarRocksPrimaryKeySink : ColumnGroupedWriteOperator
    {
        private readonly StarRocksSinkOptions _options;
        private readonly ExecutionMode executionMode;
        private readonly WriteRelation _writeRelation;
        private readonly IStarrocksClient _httpClient;
        private StarRocksJsonWriter _jsonWriter;

        private IReadOnlyList<int>? _primaryKeyOrdinals;
        private IReadOnlyList<string>? _primaryKeyColumnNames;
        private readonly string _tableName;
        private readonly string _databaseName;
        private IObjectState<long>? _labelId;
        private bool _hasUncommitedData = false;

        public StarRocksPrimaryKeySink(
            StarRocksSinkOptions options,
            ExecutionMode executionMode, 
            WriteRelation writeRelation, 
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            this._options = options;
            this.executionMode = executionMode;
            this._writeRelation = writeRelation;
            _jsonWriter = new StarRocksJsonWriter(writeRelation.TableSchema.Names);

            if (writeRelation.NamedObject.Names.Count != 2)
            {
                throw new StarRocksConfigurationException("Starrocks table name must be in the format 'database.table'");
            }

            _tableName = writeRelation.NamedObject.Names[1];
            _databaseName = writeRelation.NamedObject.Names[0];
            _httpClient = _options.ClientFactory.CreateClient(_options);
        }

        public override string DisplayName => "Starrocks";

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _labelId = await stateManagerClient.GetOrCreateObjectStateAsync<long>("labelIdCounter");

            if (executionMode == ExecutionMode.OnCheckpoint)
            {
                var label = GetLabel(_labelId.Value);

                // We always try and commit the transaction with the last label in case a crash happened between checkpoint and compact
                await _httpClient.TransactionCommit(new StarRocksTransactionId(_databaseName, _tableName, label));
            }

            var tableInfo = await _httpClient.GetTableInfo(_writeRelation.NamedObject.Names);

            if (tableInfo.PrimaryKeys.Count == 0)
            {
                throw new StarRocksConfigurationException("The target Starrocks table does not have a primary key defined.");
            }

            List<int> primaryKeyOrdinals = new List<int>();
            List<string> primaryKeyColumnNames = new List<string>();
            foreach(var primaryKey in tableInfo.PrimaryKeys)
            {
                bool foundPrimaryKey = false;
                for (int i = 0; i < _writeRelation.TableSchema.Names.Count; i++)
                {
                    if (_writeRelation.TableSchema.Names[i].Equals(primaryKey, StringComparison.OrdinalIgnoreCase))
                    {
                        primaryKeyOrdinals.Add(i);
                        primaryKeyColumnNames.Add(primaryKey);
                        foundPrimaryKey = true;
                        break;
                    }
                }
                if (!foundPrimaryKey)
                {
                    throw new StarRocksConfigurationException($"Primary key column '{primaryKey}' is not inserted into the table, all primary keys must be inserted.");
                }
            }

            _primaryKeyOrdinals = primaryKeyOrdinals;
            _primaryKeyColumnNames = primaryKeyColumnNames;
            _jsonWriter = new StarRocksJsonWriter(tableInfo.ColumnNames);
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            Debug.Assert(_primaryKeyOrdinals != null, "The primary key ordinals should have been initialized in InitializeOrRestore.");
            return ValueTask.FromResult(_primaryKeyOrdinals);
        }

        public override async Task Compact()
        {
            if (executionMode == ExecutionMode.OnCheckpoint && _hasUncommitedData)
            {
                Debug.Assert(_labelId != null);

                // Commit the transaction if we are running on checkpoint mode
                var label = GetLabel(_labelId.Value);
                var commitResult = await _httpClient.TransactionCommit(new StarRocksTransactionId(_databaseName, _tableName, label));
                if (commitResult.Status != StarRocksStatus.Ok)
                {
                    throw new StarRocksTransactionException($"Transaction commit failed with status '{commitResult.Status}' and message '{commitResult.Message}'");
                }
            }
            await base.Compact();
        }

        private string GetLabel(long id)
        {
            return $"{StreamName}_{Name}_{_databaseName}_{_tableName}_{id}";
        }

        private async Task UploadChangesOnCheckpoint(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            Debug.Assert(_primaryKeyOrdinals != null);
            Debug.Assert(_primaryKeyColumnNames != null);
            Debug.Assert(_labelId != null);

            var labelid = _labelId.Value + 1;
            var labelIdentifier = GetLabel(labelid);
            var createTransactionResponse = await _httpClient.CreateTransaction(new StarRocksTransactionId(_databaseName, _tableName, labelIdentifier));

            if (createTransactionResponse.Status != StarRocksStatus.Ok)
            {
                if (createTransactionResponse.Status == StarRocksStatus.LabelAlreadyExists)
                {
                    // Rollback if the transaction already exists
                    createTransactionResponse = await _httpClient.TransactionRollback(new StarRocksTransactionId(_databaseName, _tableName, labelIdentifier));
                    if (createTransactionResponse.Status != StarRocksStatus.Ok)
                    {
                        throw new StarRocksTransactionException("Failed to rollback existing label.");
                    }
                    // Retry create transaction
                    createTransactionResponse = await _httpClient.CreateTransaction(new StarRocksTransactionId(_databaseName, _tableName, labelIdentifier));
                    if (createTransactionResponse.Status != StarRocksStatus.Ok)
                    {
                        throw new StarRocksTransactionException("Failed to create transaction after rollback.");
                    }
                }
                else
                {
                    throw new StarRocksTransactionException(createTransactionResponse.Message);
                }
            }

            try
            {
                ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();
                Utf8JsonWriter jsonWriter = new Utf8JsonWriter(bufferWriter);
                int rowCount = 0;

                jsonWriter.WriteStartArray();
                await foreach (var row in rows)
                {
                    for (int i = 0; i < _primaryKeyOrdinals.Count; i++)
                    {
                        var keyType = row.EventBatchData.Columns[_primaryKeyOrdinals[i]].GetTypeAt(row.Index, default);
                        if (keyType == Core.ColumnStore.ArrowTypeId.Null)
                        {
                            throw new StarRocksInvalidDataException($"Received a row with primary key '{_primaryKeyColumnNames[i]}' set to 'null'");
                        }
                    }
                    _jsonWriter.WriteObject(ref jsonWriter, row.EventBatchData, row.Index, row.IsDeleted);
                    rowCount++;

                    if (rowCount >= _options.BatchSize)
                    {
                        jsonWriter.WriteEndArray();
                        jsonWriter.Flush();
                        await _httpClient.TransactionLoad(new StarRocksTransactionLoadInfo(_databaseName, _tableName, labelIdentifier, bufferWriter.WrittenMemory));
                        jsonWriter.Reset();
                        bufferWriter.Clear();
                        jsonWriter.WriteStartArray();
                        rowCount = 0;
                    }
                }
                jsonWriter.WriteEndArray();
                jsonWriter.Flush();

                if (rowCount > 0)
                {
                    await _httpClient.TransactionLoad(new StarRocksTransactionLoadInfo(_databaseName, _tableName, labelIdentifier, bufferWriter.WrittenMemory));
                }

                var prepareResponse = await _httpClient.TransactionPrepare(new StarRocksTransactionId(_databaseName, _tableName, labelIdentifier));
                if (prepareResponse.Status != StarRocksStatus.Ok)
                {
                    throw new StarRocksTransactionException(prepareResponse.Message);
                }

                // Update the label id after we prepared and commit it to the checkpoint data
                _labelId.Value = labelid;
                await _labelId.Commit();

                // Mark that we have uncommited data, this is used in the compact stage
                _hasUncommitedData = true;
            }
            catch(Exception)
            {
                // Rollback transaction in case of errors
                await _httpClient.TransactionRollback(new StarRocksTransactionId(_databaseName, _tableName, labelIdentifier));
                throw;
            }
            
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            Debug.Assert(_primaryKeyOrdinals != null);
            Debug.Assert(_primaryKeyColumnNames != null);

            if (executionMode == ExecutionMode.OnCheckpoint)
            {
                await UploadChangesOnCheckpoint(rows, watermark, isInitialData, cancellationToken);
                return;
            }
            ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();
            Utf8JsonWriter jsonWriter = new Utf8JsonWriter(bufferWriter);
            int rowCount = 0;
            jsonWriter.WriteStartArray();
            await foreach(var row in rows)
            {
                for (int i = 0; i < _primaryKeyOrdinals.Count; i++)
                {
                    var keyType = row.EventBatchData.Columns[_primaryKeyOrdinals[i]].GetTypeAt(row.Index, default);
                    if (keyType == Core.ColumnStore.ArrowTypeId.Null)
                    {
                        throw new StarRocksInvalidDataException($"Received a row with primary key '{_primaryKeyColumnNames[i]}' set to 'null'");
                    }
                }
                _jsonWriter.WriteObject(ref jsonWriter, row.EventBatchData, row.Index, row.IsDeleted);
                rowCount++;

                if(rowCount >= _options.BatchSize)
                {
                    jsonWriter.WriteEndArray();
                    jsonWriter.Flush();
                    await _httpClient.StreamLoad(new StarRocksStreamLoadInfo(_databaseName, _tableName, bufferWriter.WrittenMemory));
                    jsonWriter.Reset();
                    bufferWriter.Clear();
                    jsonWriter.WriteStartArray();
                    rowCount = 0;
                }
            }
            jsonWriter.WriteEndArray();
            jsonWriter.Flush();

            if (rowCount > 0)
            {
                await _httpClient.StreamLoad(new StarRocksStreamLoadInfo(_databaseName, _tableName, bufferWriter.WrittenMemory));
            }
        }

        protected override void Checkpoint(long checkpointTime)
        {
        }
    }
}
