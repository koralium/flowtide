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

using InlineIL;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using static InlineIL.IL.Emit;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    public unsafe static class UnsafeEx
    {
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Cle(long first, long second)
        {
            Ldarg_0();
            Ldarg_1();
            IL.Emit.Cgt();
            IL.Emit.Ldc_I4_0();
            IL.Emit.Ceq();
            Ret();
            throw IL.Unreachable();
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Clt(long first, long second)
        {
            Ldarg_0();
            Ldarg_1();
            IL.Emit.Clt();
            Ret();
            throw IL.Unreachable();
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Cgt(int first, int second)
        {
            Ldarg_0();
            Ldarg_1();
            IL.Emit.Cgt();
            Ret();
            throw IL.Unreachable();
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Cgt(long first, long second)
        {
            Ldarg_0();
            Ldarg_1();
            IL.Emit.Cgt();
            Ret();
            throw IL.Unreachable();
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Cge(long first, long second)
        {
            Ldarg_0();
            Ldarg_1();
            IL.Emit.Clt();
            IL.Emit.Ldc_I4_0();
            IL.Emit.Ceq();
            Ret();
            throw IL.Unreachable();
        }
    }
}
