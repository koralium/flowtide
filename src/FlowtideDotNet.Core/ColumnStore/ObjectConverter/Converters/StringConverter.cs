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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class StringConverter : IObjectColumnConverter
    {
        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }
            if (value.Type == ArrowTypeId.String)
            {
                return value.AsString.ToString();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj is string str)
            {
                addFunc.AddValue(new StringValue(str));
            }
            else
            {
                throw new ArgumentException("Object is not a string");
            }
        }
    }
}
