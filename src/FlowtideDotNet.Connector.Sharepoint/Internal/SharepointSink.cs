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
using FlowtideDotNet.Connector.Sharepoint.Internal.Encoders;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using improveflowtide.Sharepoint;
using improveflowtide.Sharepoint.Internal;
using Microsoft.Graph.Models;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace improveflowtide
{
    internal class SharepointSink : SimpleGroupedWriteOperator
    {
        private readonly List<string> primaryKeys;
        private readonly SharepointSinkOptions sinkOptions;
        private SharepointGraphListClient? sharepointGraphListClient;
        private readonly WriteRelation writeRelation;
        private IBPlusTree<string, string>? _existingObjectsTree;
        private List<IColumnEncoder>? encoders;
        private List<int>? primaryKeyIndices;
        private List<IColumnEncoder>? primaryKeyEncoders;
        private string? listId;

        public SharepointSink(SharepointSinkOptions sinkOptions, WriteRelation writeRelation, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, executionDataflowBlockOptions)
        {
            this.primaryKeys = sinkOptions.PrimaryKeyColumnNames;
            this.sinkOptions = sinkOptions;
            this.writeRelation = writeRelation;
        }

        public override string DisplayName => $"SharepointList(Name={writeRelation.NamedObject.DotSeperated})";

        protected override Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            primaryKeyIndices = new List<int>();
            foreach(var pk in primaryKeys)
            {
                var index = writeRelation.TableSchema.Names.FindIndex(x => x.Equals(pk, StringComparison.OrdinalIgnoreCase));

                if (index == -1)
                {
                    throw new InvalidOperationException($"Could not find primary key {pk} in table schema");
                }

                primaryKeyIndices.Add(index);
            }
            return Task.FromResult(new MetadataResult(primaryKeyIndices));
        }

        protected override async Task InitializeOrRestore(long restoreTime, SimpleWriteState? state, IStateManagerClient stateManagerClient)
        {
            await base.InitializeOrRestore(restoreTime, state, stateManagerClient);
            Debug.Assert(primaryKeyIndices != null);

            this.sharepointGraphListClient = new SharepointGraphListClient(sinkOptions, Logger);
            await sharepointGraphListClient.Initialize();
            _existingObjectsTree = await stateManagerClient.GetOrCreateTree("object_ids_tmp", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<string, string>()
            {
                Comparer = StringComparer.OrdinalIgnoreCase,
                KeySerializer = new StringSerializer(),
                ValueSerializer = new StringSerializer()
            });
            listId = await sharepointGraphListClient.GetListId(writeRelation.NamedObject.DotSeperated);
            encoders = await sharepointGraphListClient.GetColumnEncoders(writeRelation.NamedObject.DotSeperated, writeRelation.TableSchema.Names, stateManagerClient);

            primaryKeyEncoders = new List<IColumnEncoder>();
            for (int i = 0; i < primaryKeyIndices.Count; i++)
            {
                var encoder = encoders[primaryKeyIndices[i]];
                primaryKeyEncoders.Add(encoder);
            }

            // Fetch all items from sharepoint to get their ids
            await sharepointGraphListClient.IterateList(writeRelation.NamedObject.DotSeperated, primaryKeys, async (item) =>
            {
                if (item.Id == null)
                {
                    throw new InvalidOperationException("Item id should not be null");
                }
                if (item.Fields?.AdditionalData == null)
                {
                    throw new InvalidOperationException("Item fields should not be null");
                }
                var id = item.Id;
                StringBuilder stringBuilder = new StringBuilder();
                foreach (var pk in primaryKeys)
                {
                    var pkVal = item.Fields.AdditionalData.FirstOrDefault(x => x.Key.Equals(pk, StringComparison.OrdinalIgnoreCase));
                    if (pkVal.Value != null)
                    {
                        stringBuilder.Append(pkVal.Value.ToString());
                    }
                }
                await _existingObjectsTree.Upsert(stringBuilder.ToString(), id);
                return true;
            });
        }

        private string GetKeyFromRow(RowEvent row)
        {
            Debug.Assert(primaryKeyIndices != null);
            Debug.Assert(primaryKeyEncoders != null);
            StringBuilder stringBuilder = new StringBuilder();
            for(int i = 0; i < primaryKeyIndices.Count; i++)
            {
                stringBuilder.Append(primaryKeyEncoders[i].GetKeyValueFromColumn(row.GetColumn(primaryKeyIndices[i])));
            }
            return stringBuilder.ToString();
        }

        public enum Operation
        {
            Insert,
            Update,
            Delete
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(encoders != null);
            Debug.Assert(listId != null);
            Debug.Assert(_existingObjectsTree != null);
            Debug.Assert(sharepointGraphListClient != null);

            var batch = sharepointGraphListClient.NewBatch();
            List<(string key, string batchId, Operation operation)> requests = new List<(string key, string batchId, Operation operation)>();
            await foreach(var row in rows)
            {
                // Get the combined key identifer to lookup existing objects if the row already exists and needs to be upserted instead
                var pkVal = GetKeyFromRow(row.Row);
                var (found, id) = await _existingObjectsTree.GetValue(pkVal);
                
                if (!row.IsDeleted)
                {
                    var obj = new Dictionary<string, object>();
                    for (int i = 0; i < encoders.Count; i++)
                    {
                        var encoder = encoders[i];
                        var name = writeRelation.TableSchema.Names[i];
                        await encoder.AddValue(obj, name, row.Row.GetColumn(i));
                    }
                    if (found)
                    {
                        // Update
                        var req = sharepointGraphListClient.UpdateItemBatchHttpRequest(writeRelation.NamedObject.DotSeperated, id!, obj);
                        var batchId = batch.AddBatchRequestStep(req);
                        requests.Add((pkVal, batchId, Operation.Update));
                    }
                    else
                    {
                        // Insert
                        var batchReq = sharepointGraphListClient.CreateItemBatchHttpRequest(listId, obj);
                        var batchId = batch.AddBatchRequestStep(batchReq);
                        requests.Add((pkVal, batchId, Operation.Insert));
                    }
                }
                else
                {
                    //Delete
                    if (found && !sinkOptions.DisableDelete)
                    {
                        var req = sharepointGraphListClient.DeleteItemBatchHttpRequest(writeRelation.NamedObject.DotSeperated, id!);
                        var batchId = batch.AddBatchRequestStep(req);
                        requests.Add((pkVal, batchId, Operation.Delete));
                    }
                }

                if (requests.Count >= 20)
                {
                    var batchResponse = await sharepointGraphListClient.ExecuteBatch(batch);
                    var statusCodes = await batchResponse.GetResponsesStatusCodesAsync();
                    foreach(var req in requests)
                    {
                        if (statusCodes.TryGetValue(req.batchId, out var statusCode) &&
                            statusCode != System.Net.HttpStatusCode.OK &&
                            statusCode != System.Net.HttpStatusCode.Created)
                        {
                            throw new InvalidOperationException($"Failed to execute batch request {req.batchId}");   
                        }
                        if (req.operation == Operation.Insert)
                        {
                            var listItem = await batchResponse.GetResponseByIdAsync<ListItem>(req.batchId);
                            var listItemId = listItem.Id;
                            if (listItemId == null)
                            {
                                throw new InvalidOperationException($"Failed to get list item id from batch response {req.batchId}");
                            }
                            await _existingObjectsTree.Upsert(req.key, listItemId);
                        }
                    }
                    requests.Clear();
                    batch = sharepointGraphListClient.NewBatch();
                }
            }

            if (requests.Count > 0)
            {
                var batchResponse = await sharepointGraphListClient.ExecuteBatch(batch);
                var statusCodes = await batchResponse.GetResponsesStatusCodesAsync();
                foreach (var req in requests)
                {
                    
                    if (statusCodes.TryGetValue(req.batchId, out var statusCode))
                    {
                        if (statusCode == System.Net.HttpStatusCode.OK ||
                            statusCode == System.Net.HttpStatusCode.Created)
                        {
                            continue;
                        }
                    }
                    throw new InvalidOperationException($"Failed to execute batch request {req.batchId}");
                }
            }
        }
    }
}
