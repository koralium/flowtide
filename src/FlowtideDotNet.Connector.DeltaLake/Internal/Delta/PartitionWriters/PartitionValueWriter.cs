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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.PartitionWriters
{
    internal class PartitionValueWriter
    {
        private readonly IReadOnlyList<string> _partitionColumns;
        private StringBuilder _stringBuilder = new StringBuilder();
        private int[] _inputIndices;
        private IColumnPartitionWriter[] _writers;

        private Dictionary<string, PartitionValue> _values = new Dictionary<string, PartitionValue>();

        public PartitionValueWriter(StructType schema, IReadOnlyList<string> partitionColumns, List<string> columnNames)
        {
            _inputIndices = new int[partitionColumns.Count];
            _writers = new IColumnPartitionWriter[partitionColumns.Count];

            for (int i = 0; i < partitionColumns.Count; i++)
            {
                var partitionColumn = partitionColumns[i];
                var dataIndex = GetIndex(columnNames, partitionColumn);
                if (dataIndex == -1)
                {
                    throw new Exception($"Partition column {partitionColumn} not found in the input data from the query.");
                }
                _inputIndices[i] = dataIndex;
                var field = schema.Fields.First(x => x.Name.Equals(partitionColumn, StringComparison.OrdinalIgnoreCase));

                var writer = PartitionWriterVisitor.Instance.Visit(field.Type);
                _writers[i] = writer;
            }

            this._partitionColumns = partitionColumns;
        }

        private static int GetIndex(List<string> columnNames, string columnName)
        {
            for (int i = 0; i < columnNames.Count; i++)
            {
                if (columnNames[i].Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        public PartitionValue WritePartitionValue(ColumnRowReference row)
        {
            if (_partitionColumns.Count == 0)
            {
                return PartitionValue.Empty;
            }
            _stringBuilder.Clear();
            for (int i = 0; i < _inputIndices.Length; i++)
            {
                var value = row.referenceBatch.Columns[_inputIndices[i]].GetValueAt(row.RowIndex, default);
                _stringBuilder.Append(_writers[i].GetValue(value));
                if (i < _inputIndices.Length - 1)
                {
                    _stringBuilder.Append("/");
                }
            }

            var val = _stringBuilder.ToString();

            if (_values.TryGetValue(val, out var existing))
            {
                return existing;
            }

            List<KeyValuePair<string, string>> values = new List<KeyValuePair<string, string>>();
            for (int i = 0; i < _inputIndices.Length; i++)
            {
                var value = row.referenceBatch.Columns[_inputIndices[i]].GetValueAt(row.RowIndex, default);
                values.Add(new KeyValuePair<string, string>(_partitionColumns[i], _writers[i].GetValue(value)));
            }
            var partitionValue = new PartitionValue(values);
            _values[val] = partitionValue;

            return partitionValue;
        }

    }
}
