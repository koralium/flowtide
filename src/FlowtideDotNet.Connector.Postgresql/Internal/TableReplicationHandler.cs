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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    /// <summary>
    /// Handles replication from a single table.
    /// 
    /// Responsible for r
    /// </summary>
    internal class TableReplicationHandler
    {
        private readonly ReadRelation readRelation;
        private readonly IMemoryAllocator memoryAllocator;
        private SortedList<int, ReplicationConverter> _converters;

        private BitmapList[]? toastedBitmaps;

        public TableReplicationHandler(ReadRelation readRelation, IMemoryAllocator memoryAllocator)
        {
            this.readRelation = readRelation;
            this.memoryAllocator = memoryAllocator;
            _converters = new SortedList<int, ReplicationConverter>();
        }

        public ValueTask OnTableReplicationInfo(TableReplicationInfo replicationInfo)
        {
            _converters.Clear();
            // Go through the columns and compare them to the read relation columns
            for (int i = 0; i < replicationInfo.Columns.Count; i++)
            {
                var column = replicationInfo.Columns[i];
                for (int k = 0; k < readRelation.BaseSchema.Names.Count; k++)
                {
                    if (column.ColumnName.Equals(readRelation.BaseSchema.Names[k], StringComparison.OrdinalIgnoreCase))
                    {
                        var convertFunc = CreateColumnConverter(column, i, k);
                        _converters.Add(i, new ReplicationConverter(k, convertFunc));
                        break;
                    }

                }
            }
            return ValueTask.CompletedTask;
        }

        public async ValueTask OnInsertMessage(InsertMessage insertMessage)
        {
            int columnCounter = 0;
            await foreach(var col in insertMessage.NewRow)
            {
                if (_converters.TryGetValue(columnCounter, out var converter))
                {
                    var outputColumnId = converter.OutputColumn;
                    if (col.IsUnchangedToastedValue)
                    {
                        toastedBitmaps[outputColumnId].Add(true);
                        continue;
                    }
                    if (col.IsDBNull)
                    {
                        toastedBitmaps[outputColumnId].Add(false);
                        continue;
                    }
                    toastedBitmaps[outputColumnId].Add(false);
                    await converter.ConvertFunc(col, default);
                }
                columnCounter++;
            }
        }

        private Func<ReplicationValue, IColumn, ValueTask> CreateColumnConverter(ReplicationColumnInfo replicationColumn, int inputColumn, int outputColumn)
        {
            switch (replicationColumn.DataTypeName)
            {
                case "varchar":
                    return async (val, column) =>
                    {
                        var value = await val.Get<string>();
                        column.Add(new StringValue(value));
                    };
                case "int4":
                    return async (val, column) =>
                    {
                        var value = await val.Get<int>();
                        column.Add(new Int64Value(value));
                    };
                default:
                    throw new NotSupportedException($"Data type '{replicationColumn.DataTypeName}' is not supported for replication.");
            }
        }
    }
}
