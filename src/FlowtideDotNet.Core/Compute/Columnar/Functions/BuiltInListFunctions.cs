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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInListFunctions
    {
        public static void AddBuiltInListFunctions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsList.Uri, FunctionsList.ListSortAscendingNullLast, typeof(BuiltInListFunctions), nameof(ListSortAscendingNullLast));
            functionsRegister.RegisterScalarMethod(FunctionsList.Uri, FunctionsList.ListFirstDifference, typeof(BuiltInListFunctions), nameof(ListFirstDifference));
        }

        private static IDataValue ListSortAscendingNullLast<T>(T value)
            where T : IDataValue
        {
            if (value.Type == ArrowTypeId.List)
            {
                var list = value.AsList;
                IDataValue[] newList = new IDataValue[list.Count];
                for (int i = 0; i < list.Count; i++)
                {
                    newList[i] = list.GetAt(i);
                }
                
                Array.Sort(newList, SortFieldCompareCompiler.CompareAscendingNullsLastImplementation);
                return new ListValue(newList);
            }
            return NullValue.Instance;
        }

        private static IDataValue ListFirstDifference<T1, T2>(T1 x, T2 y)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x.Type == ArrowTypeId.List)
            {
                var list1 = x.AsList;
                var list1Count = list1.Count;
                if (y.Type == ArrowTypeId.List)
                {
                    var list2 = y.AsList;

                    
                    var list2Count = list2.Count;

                    var minCount = Math.Min(list1Count, list2Count);

                    for (int i = 0; i < minCount; i++)
                    {
                        var list1Value = list1.GetAt(i);
                        if (DataValueComparer.Instance.Compare(list1Value, list2.GetAt(i)) != 0)
                        {
                            return list1Value;
                        }
                    }
                    if (list1Count > list2Count)
                    {
                        return list1.GetAt(list2Count);
                    }
                }
                else
                {
                    if (list1Count > 0)
                    {
                        return list1.GetAt(0);
                    }
                }
            }
            return NullValue.Instance;
        }
    }
}
