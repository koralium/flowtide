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

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    internal class LinkTitleDecoder : BaseDecoder
    {
        public override string ColumnType => "LinkTitle";

        protected override ValueTask DecodeValue(object? item, Column column)
        {
            if (item is string str)
            {
                column.Add(new StringValue(str));
                return ValueTask.CompletedTask;
            }
            column.Add(NullValue.Instance);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask<IDataValue> DecodeDataValue(object? item)
        {
            if (item is string str)
            {
                return ValueTask.FromResult<IDataValue>(new StringValue(str));
            }
            return ValueTask.FromResult<IDataValue>(NullValue.Instance);
        }
    }
}
