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

        private static SubstraitBaseType HandleCustom(DataType.Custom dataType)
        {
            if (dataType.Name == "any")
            {
                return new AnyType();
            }
            throw new NotImplementedException($"Unknown custom data type {dataType.Name}");
        }
    }
}
