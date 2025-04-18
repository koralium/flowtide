﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    public class LongListBenchmark
    {
        [Benchmark]
        public void AddLongList()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var longList = new NativeLongList(allocator);
            for (int i = 0; i < 1_000_000; i++)
            {
                longList.Add(i);
            }
        }

        [Benchmark]
        public void DefaultList()
        {
            var list = new List<long>();
            for (int i = 0; i < 1_000_000; i++)
            {
                list.Add(i);
            }
        }
    }
}
