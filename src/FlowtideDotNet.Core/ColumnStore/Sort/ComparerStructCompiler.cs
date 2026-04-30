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

using FastExpressionCompiler;
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
    internal static class ComparerStructCompiler
    {
        private static AssemblyName s_assemblyName = new AssemblyName("DynamicSortAssembly");
        private static AssemblyBuilder s_assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
                s_assemblyName,
                AssemblyBuilderAccess.Run
            );
        private static ModuleBuilder s_moduleBuilder = s_assemblyBuilder.DefineDynamicModule("DynamicSortModule");
        private static object s_lock = new object();
        private static int _typeNameCounter = 0;


        public static Type Compile(IColumn[] columns)
        {
            lock (s_lock)
            {
                var typeBuilder = s_moduleBuilder.DefineType(
                    $"DynamicSortContext_{_typeNameCounter++}",
                    TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.SequentialLayout,
                    typeof(ValueType)
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
                ctorIl.Emit(OpCodes.Ldarg_0);
                ctorIl.Emit(OpCodes.Ldarg_1);
                ctorIl.Emit(OpCodes.Stfld, contextField);
                ctorIl.Emit(OpCodes.Ret);


                var staticMethodBuilder = typeBuilder.DefineMethod(
                    "CompareImpl",
                    MethodAttributes.Public | MethodAttributes.Static | MethodAttributes.HideBySig,
                    typeof(int),
                    new[] { typeof(SortCompareContext).MakeByRefType(), typeof(int), typeof(int) }
                );

                staticMethodBuilder.SetImplementationFlags(MethodImplAttributes.AggressiveInlining | MethodImplAttributes.AggressiveOptimization | MethodImplAttributes.Managed | MethodImplAttributes.IL);

                var staticGen = staticMethodBuilder.GetILGenerator();

                var ctxParam = Expression.Parameter(typeof(SortCompareContext).MakeByRefType(), "ctx");
                var x = Expression.Parameter(typeof(int), "x");
                var y = Expression.Parameter(typeof(int), "y");

                var block = BatchSortCompiler.Compile(columns, ctxParam, x, y);

                var success = Expression.Lambda(block, ctxParam, x, y).CompileFastToIL(staticGen);
                if (!success) throw new InvalidOperationException("FEC failed to compile.");

                var methodBuilder = typeBuilder.DefineMethod(
                    "Compare",
                    MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.HideBySig | MethodAttributes.NewSlot,
                    typeof(int),
                    new[] { typeof(int), typeof(int) }
                );

                var gen = methodBuilder.GetILGenerator();

                gen.Emit(OpCodes.Ldarg_0);
                gen.Emit(OpCodes.Ldflda, contextField);
                gen.Emit(OpCodes.Ldarg_1);
                gen.Emit(OpCodes.Ldarg_2);
                gen.Emit(OpCodes.Call, staticMethodBuilder);
                gen.Emit(OpCodes.Ret);

                return typeBuilder.CreateType();
            }
        }
    }
}
