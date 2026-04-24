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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Graph.Models;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    /// <summary>
    /// Special decoder for the id column
    /// </summary>
    internal class IdDecoder : IColumnDecoder
    {
        public string ColumnType => "ID";

        public ValueTask Decode(ListItem item, Column column)
        {
            if (item.Id != null)
            {
                column.Add(new StringValue(item.Id));
                return ValueTask.CompletedTask;
            }
            column.Add(NullValue.Instance);
            return ValueTask.CompletedTask;
        }

        public ValueTask<IDataValue> DecodeDataValue(ListItem item)
        {
            if (item.Id != null)
            {
                return ValueTask.FromResult<IDataValue>(new StringValue(item.Id));
            }
            return ValueTask.FromResult<IDataValue>(NullValue.Instance);
        }

        public Task Initialize(string name, string listId, SharepointGraphListClient client, IStateManagerClient stateManagerClient, IDictionary<string, ColumnDefinition> columns)
        {
            return Task.CompletedTask;
        }

        public Task OnNewBatch()
        {
            return Task.CompletedTask;
        }
    }
}
