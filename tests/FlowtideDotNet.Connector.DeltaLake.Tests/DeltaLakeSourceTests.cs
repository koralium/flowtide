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

using FlowtideDotNet.AcceptanceTests.Internal;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSourceTests
    {
        [Fact]
        public async Task ReadDataWithDeletionVector()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadDataWithDeletionVector), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT company, age, name FROM table_with_deletion_logs
            ");

            await stream.WaitForUpdate();

            var actual = stream.GetActualRowsAsVectors();

            Assert.Equal(100, actual.Count);

            await stream.WaitForUpdate();

            actual = stream.GetActualRowsAsVectors();

            Assert.Equal(99, actual.Count);

            await stream.WaitForUpdate();

            actual = stream.GetActualRowsAsVectors();

            Assert.Equal(98, actual.Count);
        }

        /// <summary>
        /// Check if the data is read correctly with CDC change
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ReadDataWithCdcChange()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadDataWithCdcChange), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT id, name FROM simple_table_with_cdc
            ");

            await stream.WaitForUpdate();

            var actual = stream.GetActualRowsAsVectors();

            await stream.WaitForUpdate();

            actual = stream.GetActualRowsAsVectors();

            await stream.WaitForUpdate();

            actual = stream.GetActualRowsAsVectors();
        }


        /// <summary>
        /// This query uses 0 columns, so it checks that the correct number of rows are returned
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ReadCountData()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadCountData), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT count(*) FROM table_with_deletion_logs
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            var actual = stream.GetActualRowsAsVectors();
            Assert.Equal(100, actual.Columns[0].GetValueAt(0, default).AsLong);

            await stream.WaitForUpdate();

            actual = stream.GetActualRowsAsVectors();
            Assert.Equal(99, actual.Columns[0].GetValueAt(0, default).AsLong);

            await stream.WaitForUpdate();

            actual = stream.GetActualRowsAsVectors();
            Assert.Equal(98, actual.Columns[0].GetValueAt(0, default).AsLong);
        }

        [Fact]
        public async Task ReadColumnMapping()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadColumnMapping), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT * FROM table_with_column_mapping
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            var actual = stream.GetActualRowsAsVectors();

            Assert.Equal(5, actual.Count);
        }

        [Fact]
        public async Task ReadFromCatalog()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadFromCatalog), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT * FROM catalogtest.table_with_column_mapping
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            var actual = stream.GetActualRowsAsVectors();
            Assert.Equal(5, actual.Count);
        }

        [Fact]
        public async Task ReadUtf8()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadUtf8), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT utf8 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = "0"},
                new {v = "1"},
                new {v = "2"},
                new {v = "3"},
                new {v = "4"}
            });
        }

        [Fact]
        public async Task ReadBinary()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadBinary), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT binary FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = new byte[]{ } },
                new {v = new byte[]{ 0 } },
                new {v = new byte[]{ 0, 0 } },
                new {v = new byte[]{ 0, 0, 0 } },
                new {v = new byte[]{ 0, 0, 0, 0 } }
            });
        }

        [Fact]
        public async Task ReadInt64()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadInt64), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT int64 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 0},
                new {v = 1},
                new {v = 2},
                new {v = 3},
                new {v = 4}
            });
        }

        [Fact]
        public async Task ReadInt32()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadInt32), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT int32 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 0},
                new {v = 1},
                new {v = 2},
                new {v = 3},
                new {v = 4}
            });
        }

        [Fact]
        public async Task ReadInt16()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadInt16), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT int16 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 0},
                new {v = 1},
                new {v = 2},
                new {v = 3},
                new {v = 4}
            });
        }

        [Fact]
        public async Task ReadInt8()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadInt8), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT int8 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 0},
                new {v = 1},
                new {v = 2},
                new {v = 3},
                new {v = 4}
            });
        }

        [Fact]
        public async Task ReadFloat32()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadFloat32), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT float32 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 0.0f},
                new {v = 1.0f},
                new {v = 2.0f},
                new {v = 3.0f},
                new {v = 4.0f}
            });
        }

        [Fact]
        public async Task ReadFloat64()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadFloat64), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT float64 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 0.0f},
                new {v = 1.0f},
                new {v = 2.0f},
                new {v = 3.0f},
                new {v = 4.0f}
            });
        }

        [Fact]
        public async Task ReadBool()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadBool), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT bool FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = true},
                new {v = false},
                new {v = true},
                new {v = false},
                new {v = true}
            });
        }

        [Fact]
        public async Task ReadDecimal()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadDecimal), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT decimal FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = 10.0m},
                new {v = 11.0m},
                new {v = 12.0m},
                new {v = 13.0m},
                new {v = 14.0m}
            });
        }

        [Fact]
        public async Task ReadDate()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadDate), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT date32 FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = DateTime.Parse("1970-01-01")},
                new {v = DateTime.Parse("1970-01-02")},
                new {v = DateTime.Parse("1970-01-03")},
                new {v = DateTime.Parse("1970-01-04")},
                new {v = DateTime.Parse("1970-01-05")}
            });
        }

        [Fact]
        public async Task ReadTimestamp()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadTimestamp), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT timestamp FROM all_primitive_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] {
                new {v = DateTimeOffset.Parse("1970-01-01 00:00:00+00:00")},
                new {v = DateTimeOffset.Parse("1970-01-01 01:00:00+00:00")},
                new {v = DateTimeOffset.Parse("1970-01-01 02:00:00+00:00")},
                new {v = DateTimeOffset.Parse("1970-01-01 03:00:00+00:00")},
                new {v = DateTimeOffset.Parse("1970-01-01 04:00:00+00:00")}
            });
        }

        [Fact]
        public async Task ReadStruct()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadStruct), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT struct FROM nested_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[]
            {
                new { v = new Dictionary<string, object>(){ { "bool", false }, { "float64", 1.0f } } },
                new { v = new Dictionary<string, object>(){ { "bool", false }, { "float64", 3.0f } } },
                new { v = new Dictionary<string, object>(){ { "bool", true }, { "float64", 0.0f } } },
                new { v = new Dictionary<string, object>(){ { "bool", true }, { "float64", 2.0f } } },
                new { v = new Dictionary<string, object>(){ { "bool", true }, { "float64", 4.0f } } },
            });
        }

        [Fact]
        public async Task ReadArray()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadArray), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT array FROM nested_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[]
            {
                new { v = new List<int>(){ 0 } },
                new { v = new List<int>(){ 0, 1 } },
                new { v = new List<int>(){ 0, 1, 2 } },
                new { v = new List<int>(){ 0, 1, 2, 3 } },
                new { v = new List<int>(){ 0, 1, 2, 3, 4 } },
            });
        }

        [Fact]
        public async Task ReadMap()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            DeltaLakeTestStream stream = new DeltaLakeTestStream(nameof(ReadMap), storageLoc);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT map FROM nested_types
            ", ignoreSameDataCheck: true);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[]
            {
                new { v = new Dictionary<string, object>() },
                new { v = new Dictionary<string, object>(){ { "0", 0} } },
                new { v = new Dictionary<string, object>(){ { "0", 0 }, { "1", 1 } } },
                new { v = new Dictionary<string, object>(){ { "0", 0 }, { "1", 1 }, { "2", 2 } } },
                new { v = new Dictionary<string, object>(){ { "0", 0 }, { "1", 1 }, { "2", 2 }, { "3", 3 } } },
            });
        }
    }
}
