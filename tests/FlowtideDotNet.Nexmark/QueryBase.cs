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
using FlowtideDotNet.Nexmark.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark
{
    public class QueryBase
    {
        private int iterationId = 0;
        private NexmarkQueryStream _stream = null!;

        protected NexmarkQueryStream Stream => _stream;

        [GlobalSetup]
        public void Setup()
        {
            string baseDir = GetBaseDir();
            FileDataGenerator.GenerateData(10_000_000, 10_000, baseDir);
        }

        private static string GetBaseDir()
        {
            string path = AppContext.BaseDirectory;
            string search = $"{System.IO.Path.DirectorySeparatorChar}bin{System.IO.Path.DirectorySeparatorChar}Release{System.IO.Path.DirectorySeparatorChar}";
            int index = path.IndexOf(search, StringComparison.OrdinalIgnoreCase);
            if (index >= 0)
            {
                int endOfTfm = path.IndexOf(System.IO.Path.DirectorySeparatorChar, index + search.Length);
                if (endOfTfm >= 0)
                {
                    return path.Substring(0, endOfTfm);
                }
            }
            return path;
        }

        [IterationSetup]
        public void IterationSetup()
        {
            string baseDir = GetBaseDir();
            _stream = new NexmarkQueryStream(baseDir, iterationId.ToString())
            {
                CachePageCount = 100_000
            };
            iterationId++;
        }
    }
}
