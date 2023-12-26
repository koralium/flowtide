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

using Microsoft.Win32.SafeHandles;
using System.Reflection;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.FileCache.Internal
{
    public static class PosixUnix
    {
        public enum FileAdvice : int
        {
            POSIX_FADV_NORMAL = 0,    /* no special advice, the default value */
            POSIX_FADV_RANDOM = 1,    /* random I/O access */
            POSIX_FADV_SEQUENTIAL = 2,    /* sequential I/O access */
            POSIX_FADV_WILLNEED = 3,    /* will need specified pages */
            POSIX_FADV_DONTNEED = 4,    /* don't need the specified pages */
            POSIX_FADV_NOREUSE = 5,    /* data will only be accessed once */
        }

        internal delegate int AdviceDelegate(SafeFileHandle fd, long offset, long length, FileAdvice advice);
        private static AdviceDelegate? func;

        static PosixUnix()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var interopClass = typeof(int).Assembly.GetTypes().FirstOrDefault(x => x.Name == "Interop");
                if (interopClass == null)
                {
                    throw new InvalidOperationException("Interop class not found");
                }
                var sysClass = interopClass.GetNestedTypes(BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(x => x.Name == "Sys");
                if (sysClass == null)
                {
                    throw new InvalidOperationException("Sys class not found");
                }
                var method = sysClass.GetMethods(BindingFlags.Static | BindingFlags.NonPublic).FirstOrDefault(x => x.Name == "PosixFAdvise");
                if (method == null)
                {
                    throw new InvalidOperationException("PosixFAdvise method not found");
                }
                func = method.CreateDelegate<AdviceDelegate>();
            }
        }

        public static int SetAdvice(SafeFileHandle fd, long offset, long length, FileAdvice advice)
        {
            if (func != null)
            {
                return func(fd, offset, length, advice);
            }
            return -1;
                
        }
    }
}
