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

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    internal class AttachmentsDecoder : IColumnDecoder
    {
        private string? _name;

        public string ColumnType => "Attachments";

        public ValueTask<FlxValue> Decode(ListItem item)
        {
            object? value = null;
            item.Fields?.AdditionalData?.TryGetValue(_name, out value);
            if (value is bool boolVal)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(boolVal)));
            }
            return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.Null()));
        }

        public Task Initialize(string name, string listId, SharepointGraphListClient client, IStateManagerClient stateManagerClient, IDictionary<string, ColumnDefinition> columns)
        {
            _name = name;
            return Task.CompletedTask;
        }

        public Task OnNewBatch()
        {
            return Task.CompletedTask;
        }
    }
}
