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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    /// <summary>
    /// Special decoder for the id column
    /// </summary>
    internal class IdDecoder : IColumnDecoder
    {
        public string ColumnType => "ID";

        public ValueTask<FlxValue> Decode(ListItem item)
        {
            if (item.Id != null)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(item.Id)));
            }
            return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.Null()));
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
