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

        protected override ValueTask<FlxValue> DecodeValue(object? item)
        {
            if (_isInteger)
            {
                var intVal = Convert.ToInt64(item);
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(intVal)));
            }
            if (item is int intValue)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(intValue)));
            }
            if (item is float f)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(f)));
            }
            if (item is double d)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(d)));
            }
            if (item is long l)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(l)));
            }
            if (item is decimal dec)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(dec)));
            }
            return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.Null()));
        }
    }
}
