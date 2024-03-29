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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Connector.Sharepoint.Internal.Encoders;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Substrait.Protobuf;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
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
        private ICounter<long>? _eventsCounter;

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

            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }

            this.sharepointGraphListClient = new SharepointGraphListClient(sinkOptions, StreamName, Name, Logger);
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
            Logger.LogInformation("Fetching data from sharepoint.");
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
            Logger.LogInformation("Done fetching data from sharepoint");
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
            Debug.Assert(_eventsCounter != null);

            var batch = sharepointGraphListClient.NewBatch();
            List<(string key, string batchId, Operation operation, string content)> requests = new List<(string key, string batchId, Operation operation, string content)>();

            int rowCounter = 0;
            await foreach(var row in rows)
            {
                rowCounter++;
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
                    if (sinkOptions.PreprocessRow != null)
                    {
                        sinkOptions.PreprocessRow(obj);
                    }
                    if (found)
                    {
                        // Update
                        var (req, content) = sharepointGraphListClient.UpdateItemBatchHttpRequest(writeRelation.NamedObject.DotSeperated, id!, obj);
                        var batchId = batch.AddBatchRequestStep(req);
                        requests.Add((pkVal, batchId, Operation.Update, content));
                    }
                    else
                    {
                        // Insert
                        var (batchReq, content) = sharepointGraphListClient.CreateItemBatchHttpRequest(listId, obj);
                        var batchId = batch.AddBatchRequestStep(batchReq);
                        requests.Add((pkVal, batchId, Operation.Insert, content));
                    }
                }
                else
                {
                    //Delete
                    if (found && !sinkOptions.DisableDelete)
                    {
                        var (req, content) = sharepointGraphListClient.DeleteItemBatchHttpRequest(writeRelation.NamedObject.DotSeperated, id!);
                        var batchId = batch.AddBatchRequestStep(req);
                        requests.Add((pkVal, batchId, Operation.Delete, content));
                    }
                }

                if (requests.Count >= 20)
                {
                    await SendData(requests, batch, cancellationToken);
                    requests.Clear();
                    batch = sharepointGraphListClient.NewBatch();
                }
            }

            if (requests.Count > 0)
            {
                await SendData(requests, batch, cancellationToken);
                requests.Clear();
            }
        }

        private async Task SendData(
            List<(string key, string batchId, Operation operation, string content)> requests,
            BatchRequestContentCollection batch,
            CancellationToken cancellationToken)
        {
            Debug.Assert(sharepointGraphListClient != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_existingObjectsTree != null);

            _eventsCounter.Add(requests.Count);
            int retry = 0;

            while (true)
            {
                bool hasError = false;
                Dictionary<string, System.Net.HttpStatusCode>? statusCodes = default;
                var batchResponse = await sharepointGraphListClient.ExecuteBatch(batch);
                statusCodes = await batchResponse.GetResponsesStatusCodesAsync();
                TimeSpan? retryTime = default;
                foreach (var req in requests)
                {
                    // If status code does not exist, the call has been removed from the batch
                    if (statusCodes.TryGetValue(req.batchId, out var statusCode))
                    {
                        // Check if there is any throttling response, if so, find the largest delay
                        if (statusCode == System.Net.HttpStatusCode.TooManyRequests ||
                        statusCode == System.Net.HttpStatusCode.ServiceUnavailable)
                        {
                            var resp = await batchResponse.GetResponseByIdAsync(req.batchId);
                            var delay = TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, retry) * 3));
                            if (resp.Headers.RetryAfter != null)
                            {
                                if (resp.Headers.RetryAfter.Delta.HasValue)
                                {
                                    delay = resp.Headers.RetryAfter.Delta.Value;
                                }
                                else if (resp.Headers.RetryAfter.Date.HasValue)
                                {
                                    delay = resp.Headers.RetryAfter.Date.Value - DateTime.UtcNow;
                                }
                            }
                            // Pick the largest delay
                            if (!retryTime.HasValue || delay.CompareTo(retryTime.Value) > 0)
                            {
                                retryTime = delay;
                            }
                        }
                        else if (statusCode == System.Net.HttpStatusCode.OK ||
                            statusCode == System.Net.HttpStatusCode.Created ||
                            statusCode == System.Net.HttpStatusCode.NoContent ||
                            statusCode == System.Net.HttpStatusCode.NotModified ||
                            statusCode == System.Net.HttpStatusCode.Accepted)
                        {
                            if (req.operation == Operation.Insert)
                            {
                                var listItem = await batchResponse.GetResponseByIdAsync<ListItem>(req.batchId);
                                var listItemId = listItem.Id;
                                if (listItemId == null)
                                {
                                    hasError = true;
                                    Logger.LogError("Failed to get list item id from batch response {batchId}", req.batchId);
                                    continue;
                                }
                                await _existingObjectsTree.Upsert(req.key, listItemId);
                            }
                        }
                        else
                        {
                            var resp = await batchResponse.GetResponseByIdAsync(req.batchId);
                            var respString = await resp.Content.ReadAsStringAsync(cancellationToken);
                            hasError = true;
                            Logger.LogError("Failed to execute batch request {batchId}, statusCode: {statusCode}, message: {respString}", req.batchId, statusCode.ToString(), respString);
                        }
                    }
                }

                if (hasError)
                {
                    var delay = TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, retry) * 3));
                    //Log sent data
                    for (int i = 0; i < requests.Count; i++)
                    {
                        Logger.LogError("Request {i}: {request}", i, requests[i].content);
                    }

                    await Task.Delay(delay, cancellationToken);
                    retry++;
                    if (statusCodes == null)
                    {
                        batch = batch.NewBatchWithFailedRequests(requests.Select(x => x.batchId).ToDictionary(x => x, x => System.Net.HttpStatusCode.InternalServerError));
                    }
                    else
                    {
                        batch = batch.NewBatchWithFailedRequests(statusCodes);
                    }
                    if (retry >= 10)
                    {
                        throw new InvalidOperationException("Failed after too many retries.");
                    }
                    continue;
                }

                if (retryTime.HasValue)
                {
                    Logger.LogWarning("Throttling response, waiting {time} seconds", retryTime.Value.TotalSeconds);
                    await Task.Delay(retryTime.Value, cancellationToken);
                    retry++;
                    batch = batch.NewBatchWithFailedRequests(statusCodes);
                }
                else
                {
                    break;
                }
            }
        }
    }
}
