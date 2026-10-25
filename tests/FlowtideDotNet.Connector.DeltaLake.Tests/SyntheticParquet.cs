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

using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // The Thrift compact protocol, written by hand so tests can build footers and page headers the writers never produce
    internal sealed class ThriftWriter
    {
        private readonly MemoryStream _stream = new MemoryStream();
        private readonly Stack<short> _last = new Stack<short>();

        public ThriftWriter()
        {
            _last.Push(0);
        }

        public byte[] ToArray() => _stream.ToArray();

        public ThriftWriter Byte(byte value)
        {
            _stream.WriteByte(value);
            return this;
        }

        public ThriftWriter Varint(ulong value)
        {
            while (value >= 0x80)
            {
                _stream.WriteByte((byte)(value | 0x80));
                value >>= 7;
            }
            _stream.WriteByte((byte)value);
            return this;
        }

        private static ulong Zigzag(long value) => (ulong)((value << 1) ^ (value >> 63));

        public ThriftWriter Field(short id, byte type)
        {
            var last = _last.Pop();
            var delta = id - last;
            if (delta > 0 && delta <= 15)
            {
                Byte((byte)((delta << 4) | type));
            }
            else
            {
                Byte(type);
                Varint(Zigzag(id));
            }
            _last.Push(id);
            return this;
        }

        public ThriftWriter I32(short id, int value) => Field(id, 5).Varint(Zigzag(value));

        public ThriftWriter I64(short id, long value) => Field(id, 6).Varint(Zigzag(value));

        public ThriftWriter Bool(short id, bool value) => Field(id, value ? (byte)1 : (byte)2);

        public ThriftWriter Binary(short id, byte[] value) => Field(id, 8).Varint((ulong)value.Length).Bytes(value);

        public ThriftWriter String(short id, string value) => Binary(id, Encoding.UTF8.GetBytes(value));

        public ThriftWriter Bytes(byte[] value)
        {
            _stream.Write(value);
            return this;
        }

        public ThriftWriter List(short id, byte elementType, int count)
        {
            Field(id, 9);
            if (count < 15)
            {
                return Byte((byte)((count << 4) | elementType));
            }
            return Byte((byte)(0xF0 | elementType)).Varint((ulong)count);
        }

        public ThriftWriter BeginStruct(short id)
        {
            Field(id, 12);
            return BeginElement();
        }

        // A struct inside a list has no field header
        public ThriftWriter BeginElement()
        {
            _last.Push(0);
            return this;
        }

        public ThriftWriter End()
        {
            Byte(0);
            _last.Pop();
            return this;
        }

        public ThriftWriter I32Element(int value) => Varint(Zigzag(value));

        public ThriftWriter StringElement(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            return Varint((ulong)bytes.Length).Bytes(bytes);
        }
    }

    // A Parquet file assembled from schema elements, column chunks of raw pages, and a hand-built footer
    internal sealed class SyntheticParquet
    {
        public sealed record Element(string Name, int? Type = null, int Repetition = 1, int Children = 0, int? Converted = null);

        public sealed class Chunk
        {
            public required string[] Path { get; init; }

            public int Type { get; init; } = 6;

            public int Codec { get; init; }

            public int[] Encodings { get; init; } = new[] { 0, 3 };

            public long NumValues { get; init; }

            public List<byte[]> Pages { get; init; } = new List<byte[]>();

            // Sizes from the pages unless set
            public long? TotalCompressed { get; set; }

            public long? TotalUncompressed { get; set; }

            public long? DataPageOffset { get; set; }

            public long? DictionaryPageOffset { get; set; }

            public string? FilePath { get; init; }

            public byte[]? StatisticsMax { get; init; }

            public long UncompressedExtra { get; init; }
        }

        public List<Element> Schema { get; } = new List<Element>();

        public List<List<Chunk>> RowGroups { get; } = new List<List<Chunk>>();

        public List<(string Key, string Value)> KeyValues { get; } = new List<(string, string)>();

        public bool Encrypted { get; set; }

        // Rows per row group, and the file's total
        public long Rows { get; set; } = 1;

        // Row groups written without their column list, which a reader cannot do without
        public bool OmitColumns { get; set; }

        // A file total other than the sum of the row groups
        public long? FileRows { get; set; }

        // Leaves out FileMetaData's required version
        public bool OmitVersion { get; set; }

        // A page: header bytes then a body of the given length
        public static byte[] Page(int type, int compressed, int uncompressed, Action<ThriftWriter> subHeader, short subHeaderId)
        {
            var header = Header(type, compressed, uncompressed, subHeader, subHeaderId);
            var page = new byte[header.Length + compressed];
            header.CopyTo(page, 0);
            return page;
        }

        // The header alone, for sizes that claim a body the file never holds
        public static byte[] Header(int type, int compressed, int uncompressed, Action<ThriftWriter> subHeader, short subHeaderId)
        {
            var header = new ThriftWriter().BeginElement().I32(1, type).I32(2, uncompressed).I32(3, compressed);
            header.BeginStruct(subHeaderId);
            subHeader(header);
            header.End().End();
            return header.ToArray();
        }

        public static byte[] DataPage(int values, int bodyBytes, int encoding = 0, int definitionEncoding = 3) =>
            Page(0, bodyBytes, bodyBytes, h => h.I32(1, values).I32(2, encoding).I32(3, definitionEncoding).I32(4, 3), 5);

        public static int HeaderLength(byte[] page, int bodyBytes) => page.Length - bodyBytes;

        public byte[] Build()
        {
            var file = new MemoryStream();
            file.Write("PAR1"u8);
            foreach (var group in RowGroups)
            {
                foreach (var chunk in group)
                {
                    var start = file.Position;
                    long uncompressed = 0;
                    var first = true;
                    foreach (var page in chunk.Pages)
                    {
                        if (first)
                        {
                            chunk.DataPageOffset ??= start;
                            first = false;
                        }
                        file.Write(page);
                    }
                    chunk.DataPageOffset ??= start;
                    chunk.TotalCompressed ??= file.Position - start;
                    uncompressed = file.Position - start + chunk.UncompressedExtra;
                    chunk.TotalUncompressed ??= uncompressed;
                }
            }
            var footer = Footer();
            file.Write(footer);
            file.Write(BitConverter.GetBytes(footer.Length));
            file.Write(Encrypted ? "PARE"u8 : "PAR1"u8);
            return file.ToArray();
        }

        private byte[] Footer()
        {
            var w = new ThriftWriter().BeginElement();
            if (!OmitVersion)
            {
                w.I32(1, 1);
            }
            w.List(2, 12, Schema.Count);
            foreach (var element in Schema)
            {
                w.BeginElement();
                if (element.Type != null)
                {
                    w.I32(1, element.Type.Value);
                }
                w.I32(3, element.Repetition);
                w.String(4, element.Name);
                if (element.Children > 0)
                {
                    w.I32(5, element.Children);
                }
                if (element.Converted != null)
                {
                    w.I32(6, element.Converted.Value);
                }
                w.End();
            }
            w.I64(3, FileRows ?? Rows * RowGroups.Count);
            w.List(4, 12, RowGroups.Count);
            foreach (var group in RowGroups)
            {
                w.BeginElement();
                if (!OmitColumns)
                {
                    w.List(1, 12, group.Count);
                }
                foreach (var chunk in OmitColumns ? new List<Chunk>() : group)
                {
                    w.BeginElement();
                    if (chunk.FilePath != null)
                    {
                        w.String(1, chunk.FilePath);
                    }
                    w.I64(2, 0);
                    w.BeginStruct(3);
                    w.I32(1, chunk.Type);
                    w.List(2, 5, chunk.Encodings.Length);
                    foreach (var encoding in chunk.Encodings)
                    {
                        w.I32Element(encoding);
                    }
                    w.List(3, 8, chunk.Path.Length);
                    foreach (var part in chunk.Path)
                    {
                        w.StringElement(part);
                    }
                    w.I32(4, chunk.Codec);
                    w.I64(5, chunk.NumValues);
                    w.I64(6, chunk.TotalUncompressed ?? 0);
                    w.I64(7, chunk.TotalCompressed ?? 0);
                    w.I64(9, chunk.DataPageOffset ?? 4);
                    if (chunk.DictionaryPageOffset != null)
                    {
                        w.I64(11, chunk.DictionaryPageOffset.Value);
                    }
                    if (chunk.StatisticsMax != null)
                    {
                        w.BeginStruct(12).Binary(5, chunk.StatisticsMax).End();
                    }
                    w.End();
                    w.End();
                }
                w.I64(2, 0);
                w.I64(3, Rows);
                w.End();
            }
            if (KeyValues.Count > 0)
            {
                w.List(5, 12, KeyValues.Count);
                foreach (var (key, value) in KeyValues)
                {
                    w.BeginElement().String(1, key).String(2, value).End();
                }
            }
            w.End();
            return w.ToArray();
        }
    }
}
