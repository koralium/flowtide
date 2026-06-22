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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using Stowage;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ParquetSharpReaderTests
    {
        [Fact]
        public async Task TestReadAddRemovedDataFile()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(storageLoc, "/table_with_deletion_logs");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "company", "age", "name" });

            var file = table.AddFiles[0];

            var changedIndices = new List<(long id, int weight)>
            {
                (5, 1),
                (12, -1)
            };

            var allocator = FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance;
            var resultList = new List<BatchResultWithWeights>();
            await foreach (var result in reader.ReadAddRemovedDataFile(storageLoc, "/table_with_deletion_logs", file.Path!, changedIndices, file.PartitionValues, allocator))
            {
                resultList.Add(result);
            }

            Assert.Single(resultList);
            var batch = resultList[0];
            Assert.Equal(2, batch.count);
            Assert.Equal(1, batch.weights[0]);
            Assert.Equal(-1, batch.weights[1]);

            Assert.Equal(3, batch.data.Columns.Count);

            batch.data.Dispose();
            batch.weights.Dispose();
        }

        [Fact]
        public async Task TestReadAddRemovedDataFile_IndexOutOfBounds()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(storageLoc, "/table_with_deletion_logs");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "company", "age", "name" });

            var file = table.AddFiles[0];

            var changedIndices = new List<(long id, int weight)>
            {
                (100, 1)
            };

            var allocator = FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance;
            var resultList = new List<BatchResultWithWeights>();
            await foreach (var result in reader.ReadAddRemovedDataFile(storageLoc, "/table_with_deletion_logs", file.Path!, changedIndices, file.PartitionValues, allocator))
            {
                resultList.Add(result);
            }

            Assert.Empty(resultList);
        }

        [Fact]
        public async Task TestReadAddRemovedDataFile_MultiBatch()
        {
            var realStorage = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(realStorage, "/table_with_deletion_logs");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "company", "age", "name" });

            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("company", new StringType(), true))
                .Field(new Field("age", new DoubleType(), true))
                .Field(new Field("name", new StringType(), true))
                .Build();

            RecordBatch CreateBatch(int startIndex, int count)
            {
                var companyBuilder = new StringArray.Builder();
                var ageBuilder = new DoubleArray.Builder();
                var nameBuilder = new StringArray.Builder();

                for (int i = 0; i < count; i++)
                {
                    companyBuilder.Append($"company_{startIndex + i}");
                    ageBuilder.Append(startIndex + i);
                    nameBuilder.Append($"name_{startIndex + i}");
                }

                return new RecordBatch(
                    arrowSchema,
                    [
                        companyBuilder.Build(),
                        ageBuilder.Build(),
                        nameBuilder.Build()
                    ],
                    count);
            }

            var batch1 = CreateBatch(0, 30000);
            var batch2 = CreateBatch(30000, 30000);
            var batch3 = CreateBatch(60000, 30000);

            var storageLoc = Files.Of.InternalMemory("./test_multibatch");
            var filePath = "multi_batch.parquet";
            
            using (var stream = await storageLoc.OpenWrite(filePath))
            {
                using (var writer = new ParquetSharp.Arrow.FileWriter(stream, arrowSchema))
                {
                    writer.WriteRecordBatch(batch1);
                    writer.WriteRecordBatch(batch2);
                    writer.WriteRecordBatch(batch3);
                    writer.Close();
                }
            }

            var changedIndices = new List<(long id, int weight)>
            {
                (5000, 1),
                (65536, 1),
                (75000, -1)
            };

            var allocator = FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance;
            var resultList = new List<BatchResultWithWeights>();
            await foreach (var result in reader.ReadAddRemovedDataFile(storageLoc, "/", filePath, changedIndices, null, allocator))
            {
                resultList.Add(result);
            }

            Assert.Equal(2, resultList.Count);

            Assert.Equal(1, resultList[0].count);
            Assert.Equal(1, resultList[0].weights[0]);
            var (array1, _) = resultList[0].data.Columns[0].ToArrowArray();
            var companyCol1 = (Apache.Arrow.StringArray)array1;
            Assert.Equal("company_5000", companyCol1.GetString(0));

            Assert.Equal(2, resultList[1].count);
            Assert.Equal(1, resultList[1].weights[0]);
            Assert.Equal(-1, resultList[1].weights[1]);
            
            var (array2, _) = resultList[1].data.Columns[0].ToArrowArray();
            var companyCol2 = (Apache.Arrow.StringArray)array2;
            Assert.Equal("company_65536", companyCol2.GetString(0));
            Assert.Equal("company_75000", companyCol2.GetString(1));
        }
    }
}
