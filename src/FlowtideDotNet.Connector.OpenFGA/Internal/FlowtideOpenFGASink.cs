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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class FlowtideOpenFGASink : SimpleGroupedWriteOperator
    {
        private readonly int m_userIndex;
        private readonly int m_relationIndex;
        private readonly int m_objectIndex;
        private readonly int m_authorizationModelIdIndex;
        private readonly List<int> m_primaryKeys;
        private readonly OpenFGASinkOptions m_openFGASinkOptions;
        private OpenFgaClient? m_openFgaClient;

        public FlowtideOpenFGASink(OpenFGASinkOptions openFGASinkOptions, WriteRelation writeRelation, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionMode, executionDataflowBlockOptions)
        {
            m_userIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("user", StringComparison.OrdinalIgnoreCase));
            if (m_userIndex == -1)
            {
                throw new InvalidOperationException("OpenFGA requires a user field");
            }
            m_relationIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("relation", StringComparison.OrdinalIgnoreCase));
            if (m_relationIndex == -1)
            {
                throw new InvalidOperationException("OpenFGA requires a relation field");
            }
            m_objectIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("object", StringComparison.OrdinalIgnoreCase));
            if (m_objectIndex == -1)
            {
                throw new InvalidOperationException("OpenFGA requires a object field");
            }
            m_authorizationModelIdIndex = writeRelation.TableSchema.Names.FindIndex(x => x.Equals("authorizationModelId", StringComparison.OrdinalIgnoreCase));
            m_primaryKeys = new List<int>() { m_userIndex, m_relationIndex, m_objectIndex };
            this.m_openFGASinkOptions = openFGASinkOptions;
        }

        public override string DisplayName => "OpenFGASink";

        protected override Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            m_openFgaClient = new OpenFgaClient(m_openFGASinkOptions.ClientConfiguration);
            return Task.FromResult(new MetadataResult(m_primaryKeys));
        }

        private string ColumnToString(scoped in FlxValueRef flxValue)
        {
            switch (flxValue.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    return flxValue.AsBool.ToString();
                case FlexBuffers.Type.Null:
                    return "null";
                case FlexBuffers.Type.Decimal:
                    return flxValue.AsDecimal.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.Float:
                    return flxValue.AsDouble.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.Map:
                    return flxValue.AsMap.ToJson;
                case FlexBuffers.Type.Int:
                    return flxValue.AsLong.ToString(CultureInfo.InvariantCulture);
                case FlexBuffers.Type.String:
                    return flxValue.AsString;
                default:
                    throw new InvalidOperationException($"Unsupported type {flxValue.ValueType}");
            }
        }

        private ClientTupleKey GetClientTupleKey(SimpleChangeEvent row)
        {
            var userColumn = row.Row.GetColumnRef(m_userIndex);
            var relationColumn = row.Row.GetColumnRef(m_relationIndex);
            var objectColumn = row.Row.GetColumnRef(m_objectIndex);

            return new ClientTupleKey()
            {
                User = ColumnToString(userColumn),
                Relation = ColumnToString(relationColumn),
                Object = ColumnToString(objectColumn)
            };
        }

        private ClientTupleKeyWithoutCondition GetClientTupleKeyWithoutCondition(SimpleChangeEvent row)
        {
            var userColumn = row.Row.GetColumnRef(m_userIndex);
            var relationColumn = row.Row.GetColumnRef(m_relationIndex);
            var objectColumn = row.Row.GetColumnRef(m_objectIndex);

            return new ClientTupleKeyWithoutCondition()
            {
                User = ColumnToString(userColumn),
                Relation = ColumnToString(relationColumn),
                Object = ColumnToString(objectColumn)
            };
        }

        private string GetAuthorizationModelId(SimpleChangeEvent row)
        {
            var authorizationModelIdColumn = row.Row.GetColumnRef(m_authorizationModelIdIndex);
            return ColumnToString(authorizationModelIdColumn);
        }

        protected override async Task OnInitialDataSent()
        {
            if (m_openFGASinkOptions.OnInitialDataSentFunc != null)
            {
                await m_openFGASinkOptions.OnInitialDataSentFunc();
            }
            await base.OnInitialDataSent();
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(m_openFgaClient != null);

            int count = 0;
            List<Task> tasks = new List<Task>();
            await foreach(var row in rows)
            {
                count++;
                IClientWriteOptions? clientWriteOptions = default;
                if (m_authorizationModelIdIndex >= 0)
                {
                    clientWriteOptions = new ClientWriteOptions
                    {
                        AuthorizationModelId = GetAuthorizationModelId(row)
                    };
                }
                if (row.IsDeleted)
                {
                    var tuple = GetClientTupleKeyWithoutCondition(row);

                    Task? task = null;
                    if (m_openFGASinkOptions.BeforeDeleteFunc != null)
                    {
                        task = m_openFGASinkOptions.BeforeDeleteFunc(tuple)
                            .ContinueWith(async (result) =>
                            {
                                var c = await result;
                                if (!c)
                                {
                                    return;
                                }
                                await m_openFgaClient.Write(new ClientWriteRequest()
                                {
                                    Deletes = new List<ClientTupleKeyWithoutCondition>() { tuple }
                                }, clientWriteOptions);
                            })
                            .ContinueWith(x =>
                            {
                                if (x.IsFaulted)
                                {
                                    Logger.LogError(x.Exception, "Exception deleting from store");
                                }
                            });
                    }
                    else
                    {
                        task = m_openFgaClient.Write(new ClientWriteRequest()
                        {
                            Deletes = new List<ClientTupleKeyWithoutCondition>() { tuple }
                        }, clientWriteOptions)
                        .ContinueWith(x =>
                        {
                            if (x.IsFaulted)
                            {
                                Logger.LogError(x.Exception, "Exception deleting from store");
                            }
                        });
                    }

                    tasks.Add(task);
                }
                else
                {
                    var tuple = GetClientTupleKey(row);

                    Task? task = null;
                    if (m_openFGASinkOptions.BeforeWriteFunc != null)
                    {
                        task = m_openFGASinkOptions.BeforeWriteFunc(tuple)
                            .ContinueWith(async (result) =>
                            {
                                var c = await result;
                                if (!c)
                                {
                                    return;
                                }
                                await m_openFgaClient.Write(new ClientWriteRequest()
                                {
                                    Writes = new List<ClientTupleKey>() { tuple }
                                }, clientWriteOptions);
                            })
                            .ContinueWith(x =>
                            {
                                if (x.IsFaulted)
                                {
                                    Logger.LogError(x.Exception, "Exception deleting from store");
                                }
                            });
                    }
                    else
                    {
                        task = m_openFgaClient.Write(new ClientWriteRequest()
                        {
                            Writes = new List<ClientTupleKey>() { tuple }
                        }, clientWriteOptions)
                        .ContinueWith(x =>
                        {
                            if (x.IsFaulted)
                            {
                                Logger.LogError(x.Exception, "Exception deleting from store");
                                if (x.Exception != null)
                                {
                                    throw x.Exception;
                                }
                                else
                                {
                                    throw new InvalidOperationException("Unknown error sending data to OpenFGA");
                                }
                                
                            }
                        });
                    }

                    tasks.Add(task);
                }

                while (tasks.Count > m_openFGASinkOptions.MaxParallellCalls)
                {
                    for (int i = 0; i < tasks.Count; i++)
                    {
                        if (tasks[i].IsCompleted)
                        {
                            tasks.RemoveAt(i);
                        }
                    }
                    if (tasks.Count > m_openFGASinkOptions.MaxParallellCalls)
                    {
                        await Task.WhenAny(tasks);
                    }
                }
            }

            await Task.WhenAll(tasks);

            if (m_openFGASinkOptions.OnWatermarkFunc != null)
            {
                await m_openFGASinkOptions.OnWatermarkFunc(watermark);
            }
        }
    }
}
