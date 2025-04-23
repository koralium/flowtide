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

using FlowtideDotNet.Substrait.Type;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Tests
{
    public class CsvSourceTests
    {
        [Fact]
        public async Task TestCsvSource()
        {
            var csvFileOptions = new CsvFileOptions
            {
                CsvColumns = new List<string> { "c1", "c2", "c3" },
                FileStorage = Stowage.Files.Of.LocalDisk("./"),
                GetInitialFiles = () =>
                {
                    return Task.FromResult<IEnumerable<string>>(new List<string>() { "test.csv" });
                }
            };

            var testStream = new CsvTestStream("TestCsvSource", csvFileOptions);

            await testStream.StartStream(@"
                INSERT INTO output
                SELECT c1, c2, c3 FROM test");

            await testStream.WaitForUpdate();

            testStream.AssertCurrentDataEqual(new[]
            {
                new { c1 = "hello", c2 = "world", c3 = "123" },
                new { c1 = "world", c2 = "hello", c3 = "321" },
            });
        }
    }
}
