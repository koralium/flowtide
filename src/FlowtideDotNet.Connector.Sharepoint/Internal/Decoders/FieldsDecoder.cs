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
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Graph.Models;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    /// <summary>
    /// Special column, contains all other columns in a map structure.
    /// This column can be used to handle columns in a more dynamic way.
    /// </summary>
    internal class FieldsDecoder : IColumnDecoder
    {
        private Dictionary<string, IColumnDecoder>? _decoders;
        private readonly Dictionary<string, string?> _descriptions = new Dictionary<string, string?>();
        private HashSet<string>? _columns;
        private SharepointGraphListClient? _client;
        private string? _listId;
        private IStateManagerClient? _stateManagerClient;

        public string ColumnType => "Fields";

        private async ValueTask<List<KeyValuePair<IDataValue, IDataValue>>> GetKeyValues(ListItem listItem)
        {
            Debug.Assert(_decoders != null);

            List<KeyValuePair<IDataValue, IDataValue>> pairs = new List<KeyValuePair<IDataValue, IDataValue>>();

            foreach (var decoder in _decoders)
            {
                var val = await decoder.Value.DecodeDataValue(listItem);

                List<KeyValuePair<IDataValue, IDataValue>> valuesPairs =
                [
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("value"), val),
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue("type"), new StringValue(decoder.Value.ColumnType)),
                ];

                if (_descriptions.TryGetValue(decoder.Key, out var desc) && desc != null)
                {
                    valuesPairs.Add(new KeyValuePair<IDataValue, IDataValue>(new StringValue("description"), new StringValue(desc)));
                }
                pairs.Add(new KeyValuePair<IDataValue, IDataValue>(new StringValue(decoder.Key), new MapValue(valuesPairs)));
            }
            return pairs;
        }

        public async ValueTask Decode(ListItem item, Column column)
        {
            var pairs = await GetKeyValues(item);
            column.Add(new MapValue(pairs));
        }

        public async ValueTask<IDataValue> DecodeDataValue(ListItem item)
        {
            var pairs = await GetKeyValues(item);
            return new MapValue(pairs);
        }


        private async Task RefetchColumns()
        {
            Debug.Assert(_client != null);
            Debug.Assert(_listId != null);
            Debug.Assert(_stateManagerClient != null);

            var columns = await _client.GetColumns(_listId);
            _decoders = await _client.GetColumnDecoders(_listId, columns.Keys.ToList(), _stateManagerClient);
            _columns = columns.Keys.ToHashSet();
            foreach (var column in columns)
            {
                _descriptions[column.Key] = column.Value.Description;
            }
        }

        public async Task Initialize(string name, string listId, SharepointGraphListClient client, IStateManagerClient stateManagerClient, IDictionary<string, ColumnDefinition> columns)
        {
            _client = client;
            _listId = listId;
            _stateManagerClient = stateManagerClient;
            _decoders = await client.GetColumnDecoders(listId, columns.Keys.ToList(), stateManagerClient);
            _columns = columns.Keys.ToHashSet();
            foreach (var column in columns)
            {
                _descriptions[column.Key] = column.Value.Description;
            }
        }

        public async Task OnNewBatch()
        {
            Debug.Assert(_client != null);
            Debug.Assert(_columns != null);
            Debug.Assert(_listId != null);

            // Fetch columns on the list to see if any new columns have been added
            var columns = await _client.GetColumns(_listId);

            if (columns.Keys.Except(_columns).Any())
            {
                await RefetchColumns();
            }
        }
    }
}
