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
//
// mimalloc assertion macros (`mi_assert`, `mi_assert_internal`, `mi_assert_expensive`).
// In C these are enabled at MI_DEBUG >= 1 / 2 / 3 respectively. In this port the
// `MI_DEBUG` symbol is defined for Debug builds (see csproj) which enables
// mi_assert and mi_assert_internal; define `MI_DEBUG_FULL` manually to also run
// the expensive invariant checks.

using System.Diagnostics;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        [Conditional("MI_DEBUG")]
        public static void mi_assert(bool condition)
        {
            if (!condition)
            {
                Debug.Fail("mimalloc assertion failed");
            }
        }

        [Conditional("MI_DEBUG")]
        public static void mi_assert(bool condition, string message)
        {
            if (!condition)
            {
                Debug.Fail("mimalloc assertion failed: " + message);
            }
        }

        [Conditional("MI_DEBUG")]
        public static void mi_assert_internal(bool condition)
        {
            if (!condition)
            {
                Debug.Fail("mimalloc internal assertion failed");
            }
        }

        [Conditional("MI_DEBUG")]
        public static void mi_assert_internal(bool condition, string message)
        {
            if (!condition)
            {
                Debug.Fail("mimalloc internal assertion failed: " + message);
            }
        }

        [Conditional("MI_DEBUG_FULL")]
        public static void mi_assert_expensive(bool condition)
        {
            if (!condition)
            {
                Debug.Fail("mimalloc expensive assertion failed");
            }
        }
    }
}
