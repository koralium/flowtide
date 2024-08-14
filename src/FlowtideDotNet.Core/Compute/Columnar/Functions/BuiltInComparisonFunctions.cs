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

using FlexBuffers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInComparisonFunctions
    {
        public static void AddComparisonFunctions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.Equal, typeof(BuiltInComparisonFunctions), nameof(EqualImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.NotEqual, typeof(BuiltInComparisonFunctions), nameof(NotEqualImplementation));
        }

        private static IDataValue EqualImplementation<T1, T2>(in T1 x, in T2 y, in DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            else if (DataValueComparer.CompareTo(x, y) == 0)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(true);
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }
        }

        private static IDataValue NotEqualImplementation<T1, T2>(in T1 x, in T2 y, in DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            else if (DataValueComparer.CompareTo(x, y) != 0)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(true);
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }
        }
    }
}
