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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using Substrait.Protobuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Implementations
{
    internal class ComparisonFunctions
    {

        private class EqualsFunction
        {
            private DataValueContainer dataValueContainer = new DataValueContainer();
            private IDataValue _containerAsDataValue;

            public EqualsFunction()
            {
                _containerAsDataValue = dataValueContainer;
            }

            // Does not work
            public ref readonly IDataValue TestOther()
            {
                dataValueContainer._type = ArrowTypeId.Boolean;
                return ref _containerAsDataValue;
            }

            // This probably works better since the calling method provides the result container
            public void TestAgain<T1, T2>(ref readonly T1 x, ref readonly T2 y, ref readonly DataValueContainer result)
                where T1 : IDataValue
                where T2 : IDataValue
            {
                result._boolValue = new BoolValue(true);
                result._type = ArrowTypeId.Boolean;
            }

            public IDataValue DoSomethingMultiTypeReturn<T1, T2>(ref readonly T1 x, ref readonly T2 y)
            {
                return NullValue.Instance;
            }

            // This skips the container and takes in its value type instead, and a flag if the value is null
            // This could be more performant than the container since the container is on the heap.
            // It is possible that the value is not in the cache.
            public void TestRefVal<T1, T2>(ref T1 x, ref T2 y, ref BoolValue resultVal, ref bool isNull)
                where T1 : IDataValue
                where T2 : IDataValue
            {
                
                isNull = false;
                resultVal.value = true;
            }

            // Returns a value as a ref readonly, could also be perfomant.
            public static ref readonly BoolValue EqualsImplementation<T1, T2>(ref readonly T1 x, ref readonly T2 y, out bool isNull)
                where T1 : IDataValue
                where T2 : IDataValue
            {
                isNull = false;
                return ref BoolValue.True;
            }

            //private static void Test()
            //{
            //    var val1 = new Int64Value(1);
            //    ref readonly var boolVal = ref EqualsImplementation(ref val1, ref val1);
            //}
        }

        public static void EqualsImplementation<T1, T2>(ref readonly T1 x, ref readonly T2 y, ref readonly DataValueContainer result)
            where T1: IDataValue
            where T2: IDataValue
        {
            result._boolValue = new BoolValue(true);
            result._type = ArrowTypeId.Boolean;
            //result._boolValue = new BoolValue(x.CompareTo(y) == 0);
        }

        public static void EqualsImplementation(IColumn x, IColumn y, DataValueContainer[] results, int start, int end)
        {
            for (int i = start; i < end; i++)
            {
                results[i]._boolValue = new BoolValue(x.CompareTo(y, i, i) == 0);
                results[i]._type = ArrowTypeId.Boolean;
            }
        }
    }
}
