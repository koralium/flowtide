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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.Type;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Tests
{
    public class TextLineTests
    {
        [Fact]
        public async Task TestReadTextLines()
        {
            var stream = new TextLineTestStream(nameof(TestReadTextLines), new TextLinesFileOptions()
            {
                FileStorage = Stowage.Files.Of.LocalDisk("./"),
                GetInitialFiles = (storage, options) =>
                {
                    return Task.FromResult<IEnumerable<string>>(new List<string>() { "test.csv" });
                },
                ExtraColumns = new List<FileExtraColumn>()
                {
                    new FileExtraColumn("batchId", new Int64Type(), (fileName, batchNumber, state) =>
                    {
                        return new Int64Value(batchNumber);
                    })
                }
            });

            await stream.StartStream(@"
                INSERT INTO output
                SELECT fileName, value, batchId FROM test
            ");

            await stream.WaitForUpdate();

            var act = stream.GetActualRowsAsVectors();

            stream.AssertCurrentDataEqual(new[]
            {
                new { fileName = "test.csv", value = "hello,world,123", batchId = 1 },
                new { fileName = "test.csv", value = "world,hello,321", batchId = 1 },
            });
        }
    }
}
