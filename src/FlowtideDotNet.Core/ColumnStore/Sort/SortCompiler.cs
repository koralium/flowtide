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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Loader;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    internal static class SortCompiler
    {
        public static void Compile()
        {
            var assemblyName = new AssemblyName("DynamicSortAssembly");

            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
                assemblyName,
                AssemblyBuilderAccess.RunAndCollect
            );

            var moduleBuilder = assemblyBuilder.DefineDynamicModule("DynamicSortModule");

            // 4. Define the struct type
            var typeBuilder = moduleBuilder.DefineType(
                "DynamicSortContext",
                TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.SequentialLayout,
                typeof(ValueType) // This makes it a struct!
            );

            typeBuilder.AddInterfaceImplementation(typeof(IComparer<int>));

            var contextField = typeBuilder.DefineField(
                "Context",
                typeof(SortCompareContext),
                FieldAttributes.Public
            );

            var ctorBuilder = typeBuilder.DefineConstructor(
                MethodAttributes.Public,
                CallingConventions.Standard,
                new Type[] { typeof(SortCompareContext) }
            );

            var ctorIl = ctorBuilder.GetILGenerator();
            ctorIl.Emit(OpCodes.Ldarg_0);      // Load 'this' (the struct being constructed) onto the stack
            ctorIl.Emit(OpCodes.Ldarg_1);      // Load the first argument (SortCompareContext) onto the stack
            ctorIl.Emit(OpCodes.Stfld, contextField); // Store arg1 into this.Context
            ctorIl.Emit(OpCodes.Ret);          // Return from constructor

            var methodBuilder = typeBuilder.DefineMethod(
                "Compare",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.HideBySig | MethodAttributes.NewSlot,
                typeof(int),
                new[] { typeof(int), typeof(int) }
            );

            Expression<Func<int, int, int>> expr;
            var gen = methodBuilder.GetILGenerator();

            Span<int> a;
            a.Sort()
        }
    }
}
