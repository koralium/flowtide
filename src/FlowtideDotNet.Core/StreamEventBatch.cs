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
using FlowtideDotNet.Core.ColumnStore;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Tracing;

namespace FlowtideDotNet.Core
{
    /// <summary>
    /// Represents a batch of stream events.
    /// A schema describes the name of the columns that is used for all events in this batch.
    /// The schema does not contain the data type in the column, since that can differ between events.
    /// </summary>
    public class StreamEventBatch : IRentable
    {
        //public IReadOnlyList<RowEvent> Events { get; }

        private EventBatchWeighted? _data;
        private List<RowEvent>? _events;
        // Remove later when row events have been removed
        private int _columnCount;

        public EventBatchWeighted Data => GetData();

        public IReadOnlyList<RowEvent> Events => GetEvents();

        private IReadOnlyList<RowEvent> GetEvents()
        {
            if (_events != null)
            {
                return _events;
            }
            if (_data != null)
            {
                _events = RowEventToEventBatchData.EventBatchWeightedToRowEvents(_data);
                return _events;
            }
            throw new Exception("No events or data available");
        }

        private EventBatchWeighted GetData()
        {
            if (_data != null)
            {
                return _data;
            }
            if (_events != null)
            {
                var columnCount = _columnCount;
                if (_events.Count > 0)
                {
                    columnCount = _events[0].RowData.Length;
                }
                _data = RowEventToEventBatchData.ConvertToEventBatchData(_events, columnCount);
                _data.Rent(1);
                return _data;
            }
            throw new Exception("No events or data available");
        }

        public StreamEventBatch(EventBatchWeighted data)
        {
            _data = data;
            _columnCount = _data.EventBatchData.Columns.Count;
        }

        public StreamEventBatch(List<RowEvent> events, int columnCount)
        {
            _events = events;
            _columnCount = columnCount;
        }

        public void Rent(int count)
        {
            if (_data != null)
            {
                _data.Rent(count);
            }
        }

        public void Return()
        {
            if (_data != null)
            {
                _data.Return();
            }
            
        }
    }
}
