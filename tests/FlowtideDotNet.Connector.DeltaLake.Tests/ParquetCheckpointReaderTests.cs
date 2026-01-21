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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ParquetCheckpointReaderTests
    {

        [Fact]
        public async Task ReadCheckpointFile()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");

            var reader = new ParquetCheckpointReader();
            var data = await reader.ReadCheckpointFile(storageLoc, new IOEntry("/simple_table_with_checkpoint/_delta_log/00000000000000000010.checkpoint.parquet"));

            Assert.Equal(13, data.Count);

            var add = data[0].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-9afd9224-729f-4420-a05e-8032113a6568-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751706698, add.ModificationTime);

            add = data[1].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-e9c6df9a-e585-4c70-bc1f-de9bd8ae025b-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751703532, add.ModificationTime);

            add = data[2].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-7d239c98-d74b-4b02-b3f6-9f256992c633-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751704295, add.ModificationTime);

            add = data[3].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751716698, add.ModificationTime);

            add = data[4].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-3fa65c69-4e55-4b18-a195-5f1ae583e553-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751705952, add.ModificationTime);


            add = data[5].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-3810fbe0-9892-431d-bcfd-7de5788dfe8d-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751699515, add.ModificationTime);


            add = data[6].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-72ecc4d6-2e44-4df4-99e6-23f1ac2b7b7c-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751705065, add.ModificationTime);

            add = data[7].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-e93060ad-9c8c-4170-a9da-7c6f53f6406b-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751702758, add.ModificationTime);

            add = data[8].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-8e7dc8c1-337b-40b8-a411-46d4295da531-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751701848, add.ModificationTime);

            add = data[9].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-136c36f5-639d-4e95-bb0f-15cde3fb14eb-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751701112, add.ModificationTime);

            var protocol = data[10].Protocol;
            Assert.NotNull(protocol);
            Assert.Equal(1, protocol.MinReaderVersion);
            Assert.Equal(2, protocol.MinWriterVersion);
            Assert.Null(protocol.ReaderFeatures);
            Assert.Null(protocol.WriterFeatures);

            var metadata = data[11].MetaData;
            Assert.NotNull(metadata);
            Assert.Equal(1615751699422, metadata.CreatedTime);
            Assert.NotNull(metadata.Format);
            Assert.Equal("parquet", metadata.Format.Provider);
            Assert.Equal("cf3741a3-5f93-434f-99ac-9a4bebcdf06c", metadata.Id);
            Assert.Equal("{\"type\":\"struct\",\"fields\":[{\"name\":\"version\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}", metadata.SchemaString);

            add = data[12].Add;
            Assert.NotNull(add);
            Assert.Equal("part-00000-1abe25d3-0da6-46c5-98c1-7a69872fd797-c000.snappy.parquet", add.Path);
            Assert.Equal(442, add.Size);
            Assert.Equal(1615751700275, add.ModificationTime);
        }
    }
}
