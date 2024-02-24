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

using FlexBuffers;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Graph.Models;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Encoders
{
    internal class GroupPersonEncoder : IColumnEncoder
    {
        private SharepointGraphListClient? _client;
        private readonly bool allowMultiple;

        public GroupPersonEncoder(bool allowMultiple)
        {
            this.allowMultiple = allowMultiple;
        }

        public string GetKeyValueFromColumn(FlxValue flxValue)
        {
            throw new NotImplementedException();
        }

        public async Task AddValue(Dictionary<string, object> obj, string columnName, FlxValue flxValue)
        {
            Debug.Assert(_client != null);
            if (flxValue.IsNull)
            {
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.String)
            {
                var upn = flxValue.AsString;
                var id = await _client.EnsureUser(upn);
                if (id != null)
                {
                    if (allowMultiple)
                    {
                        obj[$"{columnName}LookupId@odata.type"] = "Collection(Edm.String)";
                        obj[$"{columnName}LookupId"] = new List<string>()
                        {
                            id.Value.ToString()
                        };
                    }
                    else
                    {
                        obj[$"{columnName}LookupId"] = id;
                    }
                }
                return;
            }
            else if (flxValue.ValueType == FlexBuffers.Type.Vector)
            {
                var vec = flxValue.AsVector;
                if (allowMultiple)
                {
                    List<string> ids = new List<string>();
                    for (int i = 0; i < vec.Length; i++)
                    {
                        var row = vec[i];
                        if (row.ValueType == FlexBuffers.Type.String)
                        {
                            var id = await _client.EnsureUser(row.AsString);
                            if (id.HasValue)
                            {
                                ids.Add(id.Value.ToString());
                            }
                        }
                    }
                    obj[$"{columnName}LookupId@odata.type"] = "Collection(Edm.String)";
                    obj[$"{columnName}LookupId"] = ids;
                }
                else if (vec.Length > 0)
                {
                    var first = vec[0];
                    if (first.ValueType == FlexBuffers.Type.String)
                    {
                        var id = await _client.EnsureUser(first.AsString);
                        obj[$"{columnName}LookupId"] = id;
                    }
                }
                return;
            }
            throw new NotSupportedException("GroupPerson columns should be a string or vector value and consist of UPN.");
        }

        public Task Initialize(string name, SharepointGraphListClient client, IStateManagerClient stateManagerClient, ColumnDefinition columnDefinition)
        {
            _client = client;
            return Task.CompletedTask;
        }
    }
}
