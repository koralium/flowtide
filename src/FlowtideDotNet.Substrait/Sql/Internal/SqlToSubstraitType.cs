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

using FlowtideDotNet.Substrait.Exceptions;
using FlowtideDotNet.Substrait.Type;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal static class SqlToSubstraitType
    {
        public static SubstraitBaseType GetType(DataType dataType)
        {
            return dataType switch
            {
                DataType.Boolean => new BoolType(),
                DataType.Int => new Int64Type(),
                DataType.BigInt => new Int64Type(),
                DataType.Float => new Fp64Type(),
                DataType.Double => new Fp64Type(),
                DataType.StringType => new StringType(),
                DataType.Date => new DateType(),
                DataType.None => new AnyType(),
                DataType.Custom => HandleCustom((dataType as DataType.Custom)!),
                _ => throw new NotImplementedException($"Unknown data type {dataType}")
            };
        }

        private static DataType StringToType(string type)
        {
            return type.ToLower() switch
            {
                "boolean" => new DataType.Boolean(),
                "int" => new DataType.Int(),
                "bigint" => new DataType.BigInt(),
                "float" => new DataType.Float(),
                "double" => new DataType.Double(),
                "string" => new DataType.StringType(),
                "date" => new DataType.Date(),
                "none" => new DataType.None(),
                _ => new DataType.Custom(type)
            };
        }

        private static DataType ParseCustom(string text)
        {
            var parenthesis = text.IndexOf("(");
            if (parenthesis == -1)
            {
                return new DataType.Custom(text);
            }
            var name = text.Substring(0, parenthesis);
            var values = text.Substring(parenthesis + 1, text.Length - parenthesis - 2).Split(new string[] { ",", " " }, StringSplitOptions.None);
            return new DataType.Custom(name, values);
        }

        private static SubstraitBaseType HandleCustom(DataType.Custom dataType)
        {
            if (dataType.Name == "any")
            {
                return new AnyType();
            }
            if (dataType.Name.ToString().Equals("struct", StringComparison.OrdinalIgnoreCase))
            {
                if (dataType.Values == null)
                {
                    throw new SubstraitParseException("Struct type must have fields");
                }
                List<string> names = new List<string>();
                List<SubstraitBaseType> types = new List<SubstraitBaseType>();

                for (int i = 0; i < dataType.Values.Count; i += 2)
                {
                    var fieldName = dataType.Values[i];
                    var fieldType = dataType.Values[i + 1];

                    var typeResult = GetType(StringToType(fieldType));
                    names.Add(fieldName);
                    types.Add(typeResult);
                }
                return new NamedStruct()
                {
                    Names = names,
                    Struct = new Struct()
                    {
                        Types = types
                    },
                    Nullable = true
                };
            }
            if (dataType.Name.ToString().Equals("list", StringComparison.OrdinalIgnoreCase))
            {
                if (dataType.Values == null)
                {
                    throw new SubstraitParseException("List type must have one field");
                }

                if (dataType.Values.Count != 1)
                {
                    throw new SubstraitParseException("List type must have one field");
                }

                var fieldType = GetType(StringToType(dataType.Values[0]));
                return new ListType(fieldType);
            }
            throw new NotImplementedException($"Unknown custom data type {dataType.Name}");
        }
    }
}
