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
using improveflowtide.Sharepoint.Internal;
using Microsoft.Graph.Models;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Encoders
{
    internal class GroupPersonEncoder : IColumnEncoder
    {
        private SharepointGraphListClient? _client;
        public string GetKeyValueFromColumn(FlxValue flxValue)
        {
            throw new NotImplementedException();
        }

        public async Task AddValue(Dictionary<string, object> obj, string columnName, FlxValue flxValue)
        {
            Debug.Assert(_client != null);
            if (flxValue.ValueType == FlexBuffers.Type.String)
            {
                var upn = flxValue.AsString;
                var id = await _client.EnsureUser(upn);
                if (id != null)
                {
                    obj[$"{columnName}LookupId"] = id;
                }
                return;
            }
            throw new NotSupportedException("GroupPerson columns should be a string value and a UPN.");
        }

        public Task Initialize(string name, SharepointGraphListClient client, IStateManagerClient stateManagerClient, ColumnDefinition columnDefinition)
        {
            _client = client;
            return Task.CompletedTask;
        }
    }
}
