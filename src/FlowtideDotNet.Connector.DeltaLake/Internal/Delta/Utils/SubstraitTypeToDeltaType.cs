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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils
{
    internal static class SubstraitTypeToDeltaType
    {
        public static Schema.Types.StructType GetSchema(NamedStruct @struct)
        {
            var result = GetDeltaType(@struct) as Schema.Types.StructType;
            if (result == null)
            {
                throw new Exception("Expected struct type");
            }
            return result;
        }

        public static SchemaBaseType GetDeltaType(SubstraitBaseType type)
        {
            if (type is AnyType)
            {
                throw new Exception("AnyType is not supported in Delta Lake");
            }
            if (type is BinaryType)
            {
                return new Schema.Types.BinaryType();
            }
            if (type is BoolType)
            {
                return new Schema.Types.BooleanType();
            }
            if (type is DateType)
            {
                return new Schema.Types.DateType();
            }
            if (type is DecimalType decimalType)
            {
                return new Schema.Types.DecimalType();
            }
            if (type is Fp32Type)
            {
                return new Schema.Types.FloatType();
            }
            if (type is Fp64Type)
            {
                return new Schema.Types.DoubleType();
            }
            if (type is Int32Type)
            {
                return new Schema.Types.IntegerType();
            }
            if (type is Int64Type)
            {
                return new Schema.Types.LongType();
            }
            if (type is StringType)
            {
                return new Schema.Types.StringType();
            }
            if (type is ListType listType)
            {
                var inner = GetDeltaType(listType.ValueType);
                return new Schema.Types.ArrayType()
                {
                    ElementType = inner
                };
            }
            if (type is MapType mapType)
            {
                var key = GetDeltaType(mapType.KeyType);
                var value = GetDeltaType(mapType.ValueType);
                return new Schema.Types.MapType(key, value, true);
            }
            if (type is NamedStruct @struct)
            {
                List<Schema.Types.StructField> fields = new List<Schema.Types.StructField>();
                for (int i = 0; i < @struct.Names.Count; i++)
                {
                    var name = @struct.Names[i];
                    var inner = GetDeltaType(@struct.Struct!.Types[i]);
                    fields.Add(new Schema.Types.StructField(name, inner, true, new Dictionary<string, object>()));
                }
                return new Schema.Types.StructType(fields);
            }
            throw new NotImplementedException(type.GetType().Name);
        }
    }
}
