﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    internal class DateTimeDecoder : BaseDecoder
    {
        public override string ColumnType => "DateTime";

        protected override ValueTask<FlxValue> DecodeValue(object? item)
        {
            if (item is DateTime dateTime)
            {
                var ms = dateTime.Subtract(DateTime.UnixEpoch).Ticks;
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(ms)));
            }
            return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.Null()));
        }
    }
}
