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
using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    /// <summary>
    /// Special column, contains all other columns in a map structure.
    /// This column can be used to handle columns in a more dynamic way.
    /// </summary>
    internal class FieldsDecoder : IColumnDecoder
    {
        private readonly FlexBuffer flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
        private Dictionary<string, IColumnDecoder>? _decoders;
        private readonly Dictionary<string, string?> _descriptions = new Dictionary<string, string?>();
        private HashSet<string>? _columns;
        private SharepointGraphListClient? _client;
        private string? _listId;
        private IStateManagerClient? _stateManagerClient;

        public string ColumnType => "Fields";

        public async ValueTask<FlxValue> Decode(ListItem item)
        {
            Debug.Assert(_decoders != null);

            flexBuffer.NewObject();
            var startVec = flexBuffer.StartVector();
            foreach (var decoder in _decoders)
            {
                flexBuffer.AddKey(decoder.Key);
                var startVecInnerMap = flexBuffer.StartVector();
                flexBuffer.AddKey("value");
                flexBuffer.Add(await decoder.Value.Decode(item));

                flexBuffer.AddKey("type");
                flexBuffer.Add(decoder.Value.ColumnType);

                if (_descriptions.TryGetValue(decoder.Key, out var desc) && desc != null)
                {
                    flexBuffer.AddKey("description");
                    flexBuffer.Add(desc);
                }
                flexBuffer.SortAndEndMap(startVecInnerMap);
            }
            flexBuffer.SortAndEndMap(startVec);
            var bytes = flexBuffer.Finish();
            return FlxValue.FromBytes(bytes);
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
