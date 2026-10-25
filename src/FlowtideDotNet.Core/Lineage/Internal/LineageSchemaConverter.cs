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

using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageSchemaConverter
    {
        public static LineageSchemaFacet ConvertToFacet(NamedStruct namedStruct)
        {
            List<LineageSchemaField> fields = new List<LineageSchemaField>();

            for (int i = 0; i < namedStruct.Names.Count; i++)
            {
                string fieldName = namedStruct.Names[i];
                SubstraitBaseType fieldType = namedStruct.Struct?.Types[i] ?? AnyType.Instance;
                fields.Add(ConvertToFacet(fieldName, fieldType));
            }
            return new LineageSchemaFacet(fields);
        }

        public static LineageSchemaFacet ConvertToFacet(IReadOnlyList<LineageColumn> columns)
        {
            List<LineageSchemaField> fields = new List<LineageSchemaField>(columns.Count);
            foreach (var column in columns)
            {
                fields.Add(ConvertToFacet(column.Name, column.Type));
            }
            return new LineageSchemaFacet(fields);
        }

        internal static string ToTypeName(SubstraitBaseType type)
        {
            return type switch
            {
                NamedStruct => "struct",
                AnyType => "any",
                BinaryType => "binary",
                BoolType => "boolean",
                DateType => "date",
                DecimalType decimalType => $"decimal({decimalType.Precision}, {decimalType.Scale})",
                Fp32Type => "float",
                Fp64Type => "double",
                Int32Type => "int",
                Int64Type => "bigint",
                ListType => "array",
                MapType => "map",
                NullType => "null",
                StringType => "string",
                TimestampType => "timestamp",
                _ => "any"
            };
        }

        private static LineageSchemaField ConvertToFacet(string name, SubstraitBaseType type)
        {
            var typeName = ToTypeName(type);
            if (type is NamedStruct namedStruct)
            {
                List<LineageSchemaField> fields = new List<LineageSchemaField>();
                for (int i = 0; i < namedStruct.Names.Count; i++)
                {
                    string fieldName = namedStruct.Names[i];
                    SubstraitBaseType fieldType = namedStruct.Struct?.Types[i] ?? AnyType.Instance;
                    fields.Add(ConvertToFacet(fieldName, fieldType));
                }
                return new LineageSchemaField(name, typeName, default, fields);
            }
            else if (type is ListType listType)
            {
                var elementFacet = ConvertToFacet("_element", listType.ValueType);
                return new LineageSchemaField(name, typeName, default, new List<LineageSchemaField> { elementFacet });
            }
            else if (type is MapType mapType)
            {
                var keyFacet = ConvertToFacet("key", mapType.KeyType);
                var valueFacet = ConvertToFacet("value", mapType.ValueType);
                return new LineageSchemaField(name, typeName, default, new List<LineageSchemaField> { keyFacet, valueFacet });
            }
            return new LineageSchemaField(name, typeName, default, null);
        }
    }
}
