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

using FlowtideDotNet.Connector.Files.Internal.CsvFiles.Parser;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Tests
{
    public class CsvParserTest
    {
        private struct DataReader : ICsvRowReader
        {
            private int RowCount;
            private Column col;

            public DataReader()
            {
                col = new Column(GlobalMemoryManager.Instance, new ColumnSizeInfo()
                {
                    DataType = ArrowTypeId.String,
                    TotalRows = 1000
                });
            }

            public bool ProcessRow(ReadOnlyMemory<byte> rowMemory, ReadOnlySpan<Range> columns)
            {
                RowCount++;
                var col1 = columns[0];
                var val = new StringValue(rowMemory.Slice(col1.Start.Value, col1.End.Value - col1.Start.Value));
                col.Add(val);

                if (col.Count >= 1000)
                {
                    col.Dispose();
                    col = new Column(GlobalMemoryManager.Instance, new ColumnSizeInfo()
                    {
                        DataType = ArrowTypeId.String,
                        TotalRows = 1000
                    });
                }
                return false;
            }
        }

        [Fact]
        public async Task Parse()
        {
            StringWriter stringWriter = new StringWriter();
            for (int i = 0; i < 10_000_000; i++)
            {
                stringWriter.WriteLine($"Column1_{i},Column2_{i},Column3_{i},Column4_{i}");
            }

            // Write to file
            File.WriteAllText("test2.csv", stringWriter.ToString());

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            using SafeFileHandle handle = File.OpenHandle("test2.csv", FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
            var pipeReader = new FilePipeReader(handle);
            var reader = new VectorizedCsvParser(pipeReader);
            var dataReader = new DataReader();

            //while (true)
            //{
            //    // 1. Fetch 4MB chunk from disk
            //    var result = await pipeReader.ReadAsync();
            //    var buffer = result.Buffer;

            //    // 2. Immediately mark the entire buffer as consumed to recycle the memory
            //    pipeReader.AdvanceTo(buffer.End);

            //    // 3. Break when the file is fully read
            //    if (result.IsCompleted)
            //    {
            //        break;
            //    }
            //}
            while (await reader.FetchMoreDataAsync())
            {
                while (reader.TryParseBatch(ref dataReader))
                {
                    // Do stuff
                }
            }
            stopwatch.Stop();
            Assert.Fail($"Parsed 10 million rows in {stopwatch.Elapsed.TotalSeconds} seconds");
            //pipeReader.Complete();
        }
    }
}
