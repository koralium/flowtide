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

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Encoders
{
    internal class TextEncoder : IColumnEncoder
    {
        public string GetKeyValueFromColumn(FlxValue flxValue)
        {
            if (flxValue.ValueType == FlexBuffers.Type.String)
            {
                return flxValue.AsString;
            }
            throw new NotSupportedException("Text columns should get string values only");
        }

        public Task AddValue(Dictionary<string, object> obj, string columnName, FlxValue flxValue)
        {
            string? val;
            if (flxValue.ValueType == FlexBuffers.Type.String)
            {
                val = flxValue.AsString;
            }
            else
            {
                val = flxValue.ToJson;
            }

            obj[columnName] = val;
            return Task.CompletedTask;
        }

        public Task Initialize(string name, SharepointGraphListClient client, IStateManagerClient stateManagerClient, ColumnDefinition columnDefinition)
        {
            return Task.CompletedTask;
        }
    }
}
