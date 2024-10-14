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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Benchmarks
{
    public class BitmapListBenchmark
    {
        private BitmapList? _bitmapList;

        [IterationSetup(Targets = [nameof(RemoveRange), nameof(RemoveRangeIterative)])]
        public void RemoveRangeIterationSetup()
        {
            _bitmapList = new BitmapList(GlobalMemoryManager.Instance);
            Random r = new Random(123);

            for (int i = 0; i < 1_000_000; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                _bitmapList.Add(val);
            }
        }

        [IterationCleanup(Targets = [nameof(RemoveRange), nameof(RemoveRangeIterative)])]
        public void RemoveRangeCleanup()
        {
            if (_bitmapList != null)
            {
                _bitmapList.Dispose();
            }
        }

        [Benchmark]
        public void RemoveRange()
        {
            _bitmapList!.RemoveRange(100, 10000);
        }

        [Benchmark]
        public void RemoveRangeIterative()
        {
            var end = 100 + 10000;
            for (int i = end; i >= 100; i--)
            {
                _bitmapList!.RemoveAt(i);
            }
        }
    }
}
