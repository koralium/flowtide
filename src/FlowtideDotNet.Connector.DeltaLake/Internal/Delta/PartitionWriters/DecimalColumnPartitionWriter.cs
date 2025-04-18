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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.PartitionWriters
{
    internal class DecimalColumnPartitionWriter : IColumnPartitionWriter
    {
        private readonly DecimalType decimalType;

        public DecimalColumnPartitionWriter(DecimalType decimalType)
        {
            this.decimalType = decimalType;
        }

        public string GetValue<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return "";
            }
            var dec = value.AsDecimal;

            // round it to the scale
            dec = Math.Round(dec, decimalType.Scale);
            return dec.ToString(CultureInfo.InvariantCulture);
        }
    }
}
