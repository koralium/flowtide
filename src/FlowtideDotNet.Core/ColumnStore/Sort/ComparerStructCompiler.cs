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
        public static Type Compile(IColumn[] columns)
        {
            var assemblyName = new AssemblyName("DynamicSortAssembly");

            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
                assemblyName,
                AssemblyBuilderAccess.Run
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


            var staticMethodBuilder = typeBuilder.DefineMethod(
                "CompareImpl",
                MethodAttributes.Public | MethodAttributes.Static | MethodAttributes.HideBySig,
                typeof(int),
                new[] { typeof(SortCompareContext).MakeByRefType(), typeof(int), typeof(int) }
            );

            staticMethodBuilder.SetImplementationFlags(MethodImplAttributes.AggressiveInlining | MethodImplAttributes.AggressiveOptimization | MethodImplAttributes.Managed | MethodImplAttributes.IL);

            var staticGen = staticMethodBuilder.GetILGenerator();

            // 2. BUILD THE EXPRESSION WITH THE BAKED TYPE (SortCompareContext)
            // We completely remove the TypeBuilder from the AST!
            var ctxParam = Expression.Parameter(typeof(SortCompareContext).MakeByRefType(), "ctx");
            var x = Expression.Parameter(typeof(int), "x");
            var y = Expression.Parameter(typeof(int), "y");

            // NOTE: Because ctxParam IS the SortCompareContext now, you don't need Expression.Field anymore!
            // Just pass ctxParam directly into your compiler.
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

            gen.Emit(OpCodes.Ldarg_0);                     // Load 'this' (ref DynamicSortContext)
            gen.Emit(OpCodes.Ldflda, contextField);        // Load address of this.Context (ref SortCompareContext)
            gen.Emit(OpCodes.Ldarg_1);                     // Load 'x'
            gen.Emit(OpCodes.Ldarg_2);                     // Load 'y'
            gen.Emit(OpCodes.Call, staticMethodBuilder);   // Call the static method FEC just built!
            gen.Emit(OpCodes.Ret);                         // Return the integer result

            var modelCode = moduleBuilder.ToCode();
            // 6. BAKE AND RETURN
            return typeBuilder.CreateType();
            //var typeCode = typeBuilder.ToCode();
            //return typeBuilder.CreateType();
        }
    }
}
