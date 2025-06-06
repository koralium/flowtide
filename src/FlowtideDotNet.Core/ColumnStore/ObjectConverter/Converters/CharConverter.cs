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
    internal class CharConverter : IObjectColumnConverter
    {
        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }
            if (value.Type == ArrowTypeId.String)
            {
                var str = value.AsString.ToString();
                if (str.Length < 1)
                {
                    return null!;
                }
                return value.AsString.ToString()[0];
            }

            throw new NotImplementedException($"Cannot convert {value.Type} to Char");
        }

        public SubstraitBaseType GetSubstraitType()
        {
            return new StringType();
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj is char c)
            {
                addFunc.AddValue(new StringValue(c.ToString()));
                return;
            }
            throw new NotImplementedException();
        }
    }
}
