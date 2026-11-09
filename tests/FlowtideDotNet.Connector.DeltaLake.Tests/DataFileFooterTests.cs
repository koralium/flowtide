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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DataFileFooterTests : IDisposable
    {
        private readonly string _path = Path.Join(Path.GetTempPath(), "flowtide-footer-" + Guid.NewGuid().ToString("N") + ".parquet");

        private static readonly Delta.StructType s_table = new Delta.StructType(new[] { new Delta.StructField("v", new Delta.StringType(), false, new Dictionary<string, object>()) }.ToList());

        public void Dispose()
        {
            File.Delete(_path);
        }

        [Fact]
        public void UncompressedPagesAreBoundedByTheirStoredBodies()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new("v", Type: 6, Repetition: 0, Converted: 0));
            var dictionary = SyntheticParquet.Page(2, 4096, 4, h => h.I32(1, 1).I32(2, 0), 7);
            var data = SyntheticParquet.Page(0, 8192, 4, h => h.I32(1, 1).I32(2, 0).I32(3, 3).I32(4, 3), 5);
            var headerBytes = dictionary.Length - 4096 + data.Length - 8192;
            parquet.RowGroups.Add(new()
            {
                new SyntheticParquet.Chunk
                {
                    Path = new[] { "v" }, Type = 6, NumValues = 1,
                    DictionaryPageOffset = 4, DataPageOffset = 4 + dictionary.Length,
                    TotalUncompressed = headerBytes + 8, Pages = new() { dictionary, data }
                }
            });
            File.WriteAllBytes(_path, parquet.Build());
            var memory = new JobMemory();

            var (scan, _) = DataFileAdmission.Preflight(_path, s_table, false, memory);
            var leaf = scan.Groups[0][0];
            memory.Release(scan.ChargedBytes);

            Assert.Equal((8192L, 8192L, 4096L, 8192L), (leaf.Page, leaf.Buffer, leaf.DictionaryPage, leaf.MaxValue));
            Assert.Equal(headerBytes + 12288, leaf.Uncompressed);
            Assert.True(leaf.Dictionary >= 4096 + 4096);
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData("version", "missing-field")]
        [InlineData("logical", "binding-preflight")]
        [InlineData("bytes", "missing-field")]
        [InlineData("offset", "missing-field")]
        [InlineData("path", "missing-field")]
        [InlineData("unit", "missing-field")]
        public void FootersArrowRefusesAreDeclinedBeforeNativeOpen(string variant, string expected)
        {
            File.WriteAllBytes(_path, Fixture(variant));
            var memory = new JobMemory();

            var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(_path, s_table, false, memory));

            Assert.Equal(expected, CompactionReasons.Name(declined.Reason));
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void FooterMemoryRefusalIsAManagedBoundDecline()
        {
            File.WriteAllBytes(_path, Fixture("valid"));
            var memory = new JobMemory(managedCap: 1024);

            var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(_path, s_table, false, memory));

            Assert.Equal(CompactionReason.ManagedBound, declined.Reason);
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData("root", "binding-preflight")]
        [InlineData("struct", "binding-preflight")]
        [InlineData("wrapper", "binding-preflight")]
        [InlineData("date-int64", "binding-preflight")]
        [InlineData("uint64-int32", "binding-preflight")]
        [InlineData("millis-int64", "binding-preflight")]
        [InlineData("micros-int32", "binding-preflight")]
        [InlineData("null-bytes", "binding-preflight")]
        [InlineData("uuid-length", "binding-preflight")]
        [InlineData("float16-length", "binding-preflight")]
        [InlineData("interval-length", "binding-preflight")]
        [InlineData("orders", "malformed")]
        [InlineData("order-type", "malformed")]
        [InlineData("subheader", "missing-field")]
        public void SchemasArrowRefusesAreDeclinedBeforeOpen(string variant, string expected)
        {
            var parquet = new SyntheticParquet() { ColumnOrders = variant switch { "orders" => 1, "order-type" => 2, _ => null }, ColumnOrdersAsBools = variant == "order-type" };
            var unread = variant switch
            {
                "date-int64" => new SyntheticParquet.Element("t", Type: 2, Converted: 6),
                "uint64-int32" => new SyntheticParquet.Element("t", Type: 1, Converted: 14),
                "millis-int64" => new SyntheticParquet.Element("t", Type: 2, Converted: 7),
                "micros-int32" => new SyntheticParquet.Element("t", Type: 1, Converted: 8),
                "null-bytes" => new SyntheticParquet.Element("t", Type: 6, Logical: 11),
                "uuid-length" => new SyntheticParquet.Element("t", Type: 7, Logical: 14, TypeLength: 8),
                "float16-length" => new SyntheticParquet.Element("t", Type: 7, Logical: 15, TypeLength: 4),
                "interval-length" => new SyntheticParquet.Element("t", Type: 7, Converted: 21, TypeLength: 8),
                _ => new SyntheticParquet.Element("t", Type: 2)
            };
            parquet.Schema.Add(new("schema", Repetition: 0, Children: 2, Converted: variant == "root" ? 0 : null));
            parquet.Schema.Add(new("v", Type: 6, Repetition: 0, Converted: 0));
            string[] other;
            if (variant == "struct")
            {
                parquet.Schema.Add(new("s", Children: 1, Converted: 0));
                parquet.Schema.Add(new("x", Type: 1));
                other = new[] { "s", "x" };
            }
            else if (variant == "wrapper")
            {
                parquet.Schema.Add(new("l", Children: 1, Converted: 3));
                parquet.Schema.Add(new("list", Repetition: 2, Children: 1, Converted: 0));
                parquet.Schema.Add(new("element", Type: 1));
                other = new[] { "l", "list", "element" };
            }
            else
            {
                parquet.Schema.Add(unread);
                other = new[] { "t" };
            }
            var first = Chunk(new[] { "v" }, 6);
            if (variant == "subheader")
            {
                first = new SyntheticParquet.Chunk() { Path = new[] { "v" }, NumValues = 1, Pages = { SyntheticParquet.Page(0, 4, 4, h => h.I32(1, 1).I32(2, 0).I32(3, 3), 5) } };
            }
            parquet.RowGroups.Add(new() { first, Chunk(other, other.Length > 1 ? 1 : unread.Type!.Value) });
            File.WriteAllBytes(_path, parquet.Build());
            var memory = new JobMemory();

            var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(_path, s_table, false, memory));

            Assert.Equal(expected, CompactionReasons.Name(declined.Reason));
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData(false, "uint32")]
        [InlineData(true, "uint32")]
        [InlineData(false, "empty-int32")]
        [InlineData(true, "empty-int64")]
        [InlineData(false, "null-int32")]
        [InlineData(true, "null-int32")]
        public void UnreadColumnsArrowReadsAreKept(bool nested, string variant)
        {
            var unused = variant switch
            {
                "uint32" => new SyntheticParquet.Element("unused", Type: 1, Repetition: 0, Converted: 13),
                "empty-int32" => new SyntheticParquet.Element("unused", Type: 1, Repetition: 0, Logical: 0),
                "empty-int64" => new SyntheticParquet.Element("unused", Type: 2, Repetition: 0, Logical: 0),
                _ => new SyntheticParquet.Element("unused", Type: 1, Repetition: 0, Logical: 11)
            };
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new("schema", Repetition: 0, Children: nested ? 1 : 2));
            if (nested)
            {
                parquet.Schema.Add(new("s", Repetition: 0, Children: 2));
            }
            parquet.Schema.Add(new("v", Type: 1, Repetition: 0));
            parquet.Schema.Add(unused);
            var prefix = nested ? new[] { "s" } : System.Array.Empty<string>();
            parquet.RowGroups.Add(new() { Chunk(prefix.Append("v").ToArray(), 1), Chunk(prefix.Append("unused").ToArray(), unused.Type!.Value) });
            File.WriteAllBytes(_path, parquet.Build());
            var v = new Delta.StructType(new[] { new Delta.StructField("v", new Delta.IntegerType(), false, new Dictionary<string, object>()) }.ToList());
            var table = nested ? new Delta.StructType(new[] { new Delta.StructField("s", v, false, new Dictionary<string, object>()) }.ToList()) : v;
            var memory = new JobMemory();

            var (scan, _) = DataFileAdmission.Preflight(_path, table, false, memory);

            Assert.Equal(nested ? new[] { 0, 1 } : new[] { 0 }, scan.ReadLeaves);
            memory.Release(scan.ChargedBytes);
        }

        [Fact]
        public void ColumnOrdersMustBeStructsForOneLeaf()
        {
            var parquet = new SyntheticParquet() { ColumnOrders = 1, ColumnOrdersAsBools = true };
            parquet.Schema.Add(new("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new("v", Type: 6, Repetition: 0, Converted: 0));
            parquet.RowGroups.Add(new() { Chunk(new[] { "v" }, 6) });
            File.WriteAllBytes(_path, parquet.Build());
            var memory = new JobMemory();

            var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(_path, s_table, false, memory));

            Assert.Contains("list of structs", declined.Message);
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void V2LevelsMustFitTheUncompressedPage()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new("v", Type: 1));
            var page = SyntheticParquet.Page(3, 20, 1, h => h.I32(1, 1).I32(2, 0).I32(3, 1).I32(4, 0).I32(5, 2).I32(6, 0), 8);
            page[^20] = 2;
            page[^19] = 1;
            parquet.RowGroups.Add(new() { new SyntheticParquet.Chunk { Path = new[] { "v" }, Type = 1, NumValues = 1, Codec = 6, TotalUncompressed = page.Length - 20 + 1, Pages = { page } } });
            File.WriteAllBytes(_path, parquet.Build());
            var table = new Delta.StructType(new[] { new Delta.StructField("v", new Delta.IntegerType(), true, new Dictionary<string, object>()) }.ToList());
            var memory = new JobMemory();

            var declined = Assert.Throws<DataFileDeclinedException>(() => DataFileAdmission.Preflight(_path, table, false, memory));

            Assert.Contains("invalid V2 page header", declined.Message);
            Assert.Equal(0, memory.Managed);
        }

        private static SyntheticParquet.Chunk Chunk(string[] path, int type) => new SyntheticParquet.Chunk() { Path = path, Type = type, NumValues = 1, Pages = { SyntheticParquet.DataPage(1, 4) } };

        // One string leaf, a required field left out or a logical member sent as an I32
        private static byte[] Fixture(string variant)
        {
            var page = SyntheticParquet.DataPage(1, 4);
            var w = new ThriftWriter().BeginElement();
            if (variant != "version")
            {
                w.I32(1, 1);
            }
            w.List(2, 12, 2).BeginElement().String(4, "schema").I32(5, 1).End();
            w.BeginElement().I32(1, variant == "unit" ? 2 : 6).I32(3, 0).String(4, "v");
            if (variant == "logical")
            {
                w.BeginStruct(10).I32(1, 0).End();
            }
            else if (variant == "unit")
            {
                w.BeginStruct(10).BeginStruct(8).Bool(1, true).BeginStruct(2).I32(1, 0).End().End().End();
            }
            else
            {
                w.BeginStruct(10).BeginStruct(1).End().End();
            }
            w.End().I64(3, 1).List(4, 12, 1).BeginElement().List(1, 12, 1).BeginElement();
            if (variant != "offset")
            {
                w.I64(2, 0);
            }
            w.BeginStruct(3).I32(1, 6).List(2, 5, 2).I32Element(0).I32Element(3);
            if (variant != "path")
            {
                w.List(3, 8, 1).StringElement("v");
            }
            w.I32(4, 0).I64(5, 1).I64(6, page.Length).I64(7, page.Length).I64(9, 4).End().End();
            if (variant != "bytes")
            {
                w.I64(2, page.Length);
            }
            w.I64(3, 1).End().End();
            var footer = w.ToArray();
            using var file = new MemoryStream();
            file.Write("PAR1"u8);
            file.Write(page);
            file.Write(footer);
            file.Write(BitConverter.GetBytes(footer.Length));
            file.Write("PAR1"u8);
            return file.ToArray();
        }
    }
}
