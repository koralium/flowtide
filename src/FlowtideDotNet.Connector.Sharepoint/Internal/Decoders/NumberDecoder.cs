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
using Microsoft.Graph.Models;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    internal class NumberDecoder : BaseDecoder
    {
        public override string ColumnType => "Number";

        private bool _isInteger;

        public NumberDecoder(NumberColumn columnInfo)
        {
            _isInteger = columnInfo.DecimalPlaces == "none";
        }

        protected override ValueTask DecodeValue(object? item, Column column)
        {
            if (_isInteger)
            {
                var intVal = Convert.ToInt64(item);
                column.Add(new Int64Value(intVal));
                return ValueTask.CompletedTask;
            }
            if (item is int intValue)
            {
                column.Add(new Int64Value(intValue));
                return ValueTask.CompletedTask;
            }
            if (item is float f)
            {
                column.Add(new DoubleValue(f));
                return ValueTask.CompletedTask;
            }
            if (item is double d)
            {
                column.Add(new DoubleValue(d));
                return ValueTask.CompletedTask;
            }
            if (item is long l)
            {
                column.Add(new Int64Value(l));
                return ValueTask.CompletedTask;
            }
            if (item is decimal dec)
            {
                column.Add(new DecimalValue(dec));
                return ValueTask.CompletedTask;
            }
            column.Add(NullValue.Instance);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask<IDataValue> DecodeDataValue(object? item)
        {
            if (_isInteger)
            {
                var intVal = Convert.ToInt64(item);
                return ValueTask.FromResult<IDataValue>(new Int64Value(intVal));
            }
            if (item is int intValue)
            {
                return ValueTask.FromResult<IDataValue>(new Int64Value(intValue));
            }
            if (item is float f)
            {
                return ValueTask.FromResult<IDataValue>(new DoubleValue(f));
            }
            if (item is double d)
            {
                return ValueTask.FromResult<IDataValue>(new DoubleValue(d));
            }
            if (item is long l)
            {
                return ValueTask.FromResult<IDataValue>(new Int64Value(l));
            }
            if (item is decimal dec)
            {
                return ValueTask.FromResult<IDataValue>(new DecimalValue(dec));
            }
            return ValueTask.FromResult<IDataValue>(NullValue.Instance);
        }
    }
}
