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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.FileCache.Internal.Unix
{
    internal static class UnixFileSystemInfo
    {
        private const int _SC_PAGESIZE = 30;

        [DllImport("libc", SetLastError = true)]
        private static extern long sysconf(int name);

        public static long GetPageSize()
        {
            return sysconf(_SC_PAGESIZE);
        }

        public static int GetAlignment(string path)
        {
            var pageSize = GetPageSize();
            return (int)pageSize;
        }
    }
}
