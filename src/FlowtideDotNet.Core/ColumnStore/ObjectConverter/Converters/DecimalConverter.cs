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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class DecimalConverter : IObjectColumnConverter
    {
        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }

            if (value.Type == ArrowTypeId.Decimal128)
            {
                return value.AsDecimal;
            }

            throw new NotImplementedException($"Can not deserialize {value.Type} to decimal");
        }

        public SubstraitBaseType GetSubstraitType()
        {
            return new DecimalType();
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj is decimal dec)
            {
                addFunc.AddValue(new DecimalValue(dec));
                return;
            }
            throw new NotImplementedException();
        }
    }
}
