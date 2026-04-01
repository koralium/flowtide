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

        private static LineageSchemaField ConvertToFacet(string name, SubstraitBaseType type)
        {
            if (type is NamedStruct namedStruct)
            {
                List<LineageSchemaField> fields = new List<LineageSchemaField>();
                for (int i = 0; i < namedStruct.Names.Count; i++)
                {
                    string fieldName = namedStruct.Names[i];
                    SubstraitBaseType fieldType = namedStruct.Struct?.Types[i] ?? AnyType.Instance;
                    fields.Add(ConvertToFacet(fieldName, fieldType));
                }
                return new LineageSchemaField(name, "struct", default, fields);
            }
            else if (type is AnyType)
            {
                return new LineageSchemaField(name, "any", default, null);
            }
            else if (type is BinaryType)
            {
                return new LineageSchemaField(name, "binary", default, null);
            }
            else if (type is BoolType)
            {
                return new LineageSchemaField(name, "boolean", default, null);
            }
            else if (type is DateType)
            {
                return new LineageSchemaField(name, "date", default, null);
            }
            else if (type is DecimalType decimalType)
            {
                return new LineageSchemaField(name, $"decimal({decimalType.Precision}, {decimalType.Scale})", default, null);
            }
            else if (type is Fp32Type)
            {
                return new LineageSchemaField(name, "float", default, null);
            }
            else if (type is Fp64Type)
            {
                return new LineageSchemaField(name, "double", default, null);
            }
            else if (type is Int32Type)
            {
                return new LineageSchemaField(name, "int", default, null);
            }
            else if (type is Int64Type)
            {
                return new LineageSchemaField(name, "bigint", default, null);
            }
            else if (type is ListType listType)
            {
                var elementFacet = ConvertToFacet("_element", listType.ValueType);
                return new LineageSchemaField(name, "array", default, new List<LineageSchemaField> { elementFacet });
            }
            else if (type is MapType mapType)
            {
                var keyFacet = ConvertToFacet("key", mapType.KeyType);
                var valueFacet = ConvertToFacet("value", mapType.ValueType);
                return new LineageSchemaField(name, "map", default, new List<LineageSchemaField> { keyFacet, valueFacet });
            }
            else if (type is NullType)
            {
                return new LineageSchemaField(name, "null", default, null);
            }
            else if (type is StringType)
            {
                return new LineageSchemaField(name, "string", default, null);
            }
            else if (type is TimestampType)
            {
                return new LineageSchemaField(name, "timestamp", default, null);
            }
            else
            {
                return new LineageSchemaField(name, "any", default, null);
            }
        }
    }
}
