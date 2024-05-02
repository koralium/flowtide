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
    internal abstract class BaseDecoder : IColumnDecoder
    {
        public abstract string ColumnType { get; }

        protected string? ColumnName { get; private set; }

        public virtual ValueTask<FlxValue> Decode(ListItem item)
        {
            object? value = null;
            item.Fields?.AdditionalData?.TryGetValue(ColumnName, out value);
            return DecodeValue(value);
        }

        protected virtual ValueTask<FlxValue> DecodeValue(object? value)
        {
            throw new NotImplementedException();
        }

        public virtual Task Initialize(string name, string listId, SharepointGraphListClient client, IStateManagerClient stateManagerClient, IDictionary<string, ColumnDefinition> columns)
        {
            ColumnName = name;
            return Task.CompletedTask;
        }

        public virtual Task OnNewBatch()
        {
            return Task.CompletedTask;
        }
    }
}
