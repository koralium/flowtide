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

using Avro;
using Avro.Generic;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest;
using FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class IcebergManifestTests
    {
        [Fact]
        public void TestWriteDictionary()
        {
            var mapLongSchema = MapSchema.CreateMap(PrimitiveSchema.Create(Schema.Type.Long));
            var mapLongNullableSchema = UnionSchema.Create([mapLongSchema, PrimitiveSchema.Create(Schema.Type.Null)]);


        }

        //[Fact]
        //public async Task WriteManifestEntry()
        //{
        //    var writer = new ManifestWriter();

        //    var snapshotId = SnapshotIdGenerator.GenerateSnapshotId();
        //    writer.AddDataFile(new Internal.Delta.Schema.Types.StructType(new List<StructField>()
        //    {
        //        new StructField("test", new StringType(), true, new Dictionary<string, object>())
        //        {
        //            PhysicalName = "test",
        //            FieldId = 1
        //        }
        //    }), new Internal.Delta.Actions.DeltaAddAction()
        //    {
        //        Path = "test"
        //    }, new Internal.Delta.Stats.DeltaStatistics()
        //    {
        //        NumRecords = 1,
        //        ValueComparers = new Dictionary<string, Internal.Delta.Stats.Comparers.IStatisticsComparer>()
        //        {
        //            {"test", new Internal.Delta.Stats.Comparers.StringStatisticsComparer(new byte[]{1,2,3}, new byte[]{4,5,6}, 0) }
        //        }
        //    }, snapshotId);

        //    writer.Finish();
        //}

        [Fact]
        public void TestWriteManifestList()
        {
            var writer = new ManifestListWriter(File.OpenWrite("list.avro"));

            writer.AddManifestFile(new ManifestFile()
            {
                ManifestPath = "test-manifest.avro",
                AddedSnapshotId = 12345,
                AddedFilesCount = 1,
                AddedRowsCount = 100,
                Content = 0,
                DeletedFilesCount = 0,
                DeletedRowsCount = 0,
                ExistingFilesCount = 5,
                ExistingRowsCount = 500,
                ManifestLength = 10,
                MinSequenceNumber = 1,
                PartitionSpecId = 3,
                SequenceNumber = 5
            });
            writer.Finish();
        }
    }
}
