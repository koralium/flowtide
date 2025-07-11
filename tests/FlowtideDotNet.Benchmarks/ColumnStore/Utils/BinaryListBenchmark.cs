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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Benchmarks.ColumnStore.Utils
{
    public class BinaryListBenchmark
    {
        public static List<byte[]> GenerateBinaryList(int count, int size)
        {
            var list = new List<byte[]>(count);
            for (int i = 0; i < count; i++)
            {
                list.Add(GenerateRandomBytes(size));
            }
            return list;
        }

        private static byte[] GenerateRandomBytes(int size)
        {
            var random = new Random();
            var bytes = new byte[size];
            random.NextBytes(bytes);
            return bytes;
        }

        private List<byte[]> toInsert = new List<byte[]>();

        [GlobalSetup]
        public void GlobalSetup()
        {
            toInsert = GenerateBinaryList(1_000_000, 100);
        }

        [Benchmark]
        public void TestInsertPointers()
        {
            var bl = new List<byte[]>();

            foreach (var item in toInsert)
            {
                bl.Add(item);
            }
        }

        [Benchmark]
        public void TestInsert()
        {
            var bl = new BinaryList(GlobalMemoryManager.Instance);

            foreach (var item in toInsert)
            {
                bl.Add(item);
            }

        }


    }
}
