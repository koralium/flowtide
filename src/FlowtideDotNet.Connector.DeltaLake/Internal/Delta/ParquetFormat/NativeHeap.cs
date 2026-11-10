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

using System.Runtime.InteropServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    // glibc keeps freed buffers resident, which the ledger never counts
    internal static class NativeHeap
    {
        private static bool s_unavailable = !OperatingSystem.IsLinux();
        private static long s_trims;

        // Trims glibc ran, for tests
        public static long Trims => Interlocked.Read(ref s_trims);

        // Returns free heap pages to the OS; false off glibc
        public static bool Trim()
        {
            if (s_unavailable)
            {
                return false;
            }
            try
            {
                malloc_trim(0);
                Interlocked.Increment(ref s_trims);
                return true;
            }
            catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
            {
                // musl has no malloc_trim
                s_unavailable = true;
                return false;
            }
        }

        [DllImport("libc.so.6", ExactSpelling = true)]
        private static extern int malloc_trim(nuint pad);
    }
}
