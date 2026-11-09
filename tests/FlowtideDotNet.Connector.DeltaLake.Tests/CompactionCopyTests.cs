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


using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using Microsoft.Extensions.Time.Testing;
using ParquetSharp;
using Stowage;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text.Json;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CompactionCopyTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero);
        private static readonly DeltaParquetCodec s_zstd = new DeltaParquetCodec(Compression.Zstd, ".zstd.parquet");
        private readonly string _root = Path.Join(Path.GetTempPath(), "cc-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly IFileStorage _local;

        public CompactionCopyTests()
        {
            Directory.CreateDirectory(Path.Join(_root, "store", "t"));
            _local = Files.Of.LocalDisk(Path.Join(_root, "store"));
        }

        public void Dispose()
        {
            _owners.Release();
            Directory.Delete(_root, true);
        }

        [Fact]
        public async Task ThreeInputsWithMixedUnitsCopyToOneOutput()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("t1", new Delta.TimestampType()), Field("t2", new Delta.TimestampType()));
            var inputs = new[]
            {
                await Input("a.parquet", Arrow(30_000, TimeUnit.Millisecond, TimeUnit.Millisecond, 0)),
                await Input("b.parquet", Arrow(30_000, TimeUnit.Microsecond, TimeUnit.Nanosecond, 100_000), Enumerable.Range(0, 3_000).Select(x => x * 10L).ToArray()),
                await Input("c.parquet", Int96(15_000, 200_000))
            };
            var survivors = inputs.SelectMany((x, i) => Enumerable.Range(0, (int)x.Rows).Where(r => i != 1 || r % 10 != 0).Select(r => (Input: i, Row: (long)r))).ToList();
            var expected = survivors.Select(x => x.Input * 100_000L + x.Row).ToList();

            var result = await Run(Bin(table, inputs, 4L << 20));

            Assert.Equal(CompactionStatus.Copied, result.Status);
            Assert.Equal(new long[] { 30_000, 27_000, 15_000 }, result.Copied.Select(x => x.Rows));
            var output = Path.Join(_root, "store", "t", result.Output!.Path);
            using (var reader = new ParquetFileReader(output))
            {
                Assert.True(reader.FileMetaData.NumRowGroups > 1);
                var units = Enumerable.Range(1, 2).Select(c => ((TimestampLogicalType)reader.FileMetaData.Schema.Column(c).LogicalType).TimeUnit);
                Assert.All(units, x => Assert.Equal(ParquetSharp.TimeUnit.Micros, x));
            }
            Assert.Equal(expected, await ReadLongs(output, 0));
            Assert.Equal(survivors.Select(x => x.Input == 2 ? x.Row : x.Row * 1_000), await ReadLongs(output, 1));
            Assert.Equal(survivors.Select(x => x.Row * 1_000), await ReadLongs(output, 2));
            using var stats = JsonDocument.Parse(result.Output.Statistics!);
            Assert.Equal((expected.Count, expected.Min(), expected.Max()), (stats.RootElement.GetProperty("numRecords").GetInt32(), stats.RootElement.GetProperty("minValues").GetProperty("id").GetInt64(), stats.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64()));
            var bounds = new[] { "t1", "t2" }.Select(x => (DateTimeOffset.Parse(stats.RootElement.GetProperty("minValues").GetProperty(x).GetString()!), DateTimeOffset.Parse(stats.RootElement.GetProperty("maxValues").GetProperty(x).GetString()!)));
            Assert.All(bounds, x => Assert.Equal((DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddMilliseconds(29_999)), x));
            Assert.All(new[] { "id", "t1", "t2" }, x => Assert.Equal(0, stats.RootElement.GetProperty("nullCount").GetProperty(x).GetInt32()));
            using (var connection = new DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:"))
            {
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT count(*), CAST(sum(id) AS BIGINT), epoch_us(min(t1)), epoch_us(max(t2)) FROM read_parquet('{output.Replace("\\", "/")}')";
                using var row = command.ExecuteReader();
                row.Read();
                Assert.Equal((expected.Count, expected.Sum(), 0L, 29_999_000L), (Convert.ToInt64(row.GetValue(0)), Convert.ToInt64(row.GetValue(1)), Convert.ToInt64(row.GetValue(2)), Convert.ToInt64(row.GetValue(3))));
            }
            Assert.True(InputSummary.BatchRows(Readmit(output, table, 4L << 20).Candidate) >= 1_024);
        }

        [Fact]
        public async Task DeclinedInputsAreNeverOpened()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("t1", new Delta.TimestampType()), Field("t2", new Delta.TimestampType()));
            var inline = new DeletionVector() { StorageType = "i", PathOrInlineDv = "abc", SizeInBytes = 10, Cardinality = 1 };
            var inputs = new[]
            {
                await Input("brotli.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 0, Compression.Brotli)),
                await Input("inline.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 0)) with { DeletionVector = inline },
                await Input("ok.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 0)),
                await Input("also.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 100))
            };
            var memory = new JobMemory();

            var result = await Run(Bin(table, inputs, 8L << 20), memory);

            Assert.Equal(CompactionStatus.Copied, result.Status);
            Assert.Equal(new[] { (0, CompactionReason.UnsupportedCodec, true), (1, CompactionReason.InlineDv, true) }, result.Declines.Select(x => (x.Input, x.Reason, x.Alone)));
            Assert.Equal(2, memory.NativeOpens);
        }

        [Fact]
        public async Task AOneInputPrefixDeclinesWithoutAWriter()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("t1", new Delta.TimestampType()), Field("t2", new Delta.TimestampType()));
            var inputs = new[]
            {
                await Input("brotli.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 0, Compression.Brotli)),
                await Input("ok.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 0))
            };
            var memory = new JobMemory();

            var result = await Run(Bin(table, inputs, 8L << 20), memory);

            Assert.Equal(CompactionStatus.Declined, result.Status);
            Assert.Empty(result.Copied);
            Assert.Equal(0, memory.NativeOpens);
            Assert.Equal(new[] { "brotli.parquet", "ok.parquet" }, Directory.GetFiles(Path.Join(_root, "store", "t")).Select(x => Path.GetFileName(x)).Order(StringComparer.Ordinal));
        }

        [Fact]
        public async Task AnInputThatBreaksThePrefixStopsTheFetch()
        {
            var hooks = HookFileStorage.Wrap(Files.Of.InternalMemory($"./cc-{Guid.NewGuid():N}"));
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var files = new[] { Strings(200_000, 8, group: 10_000), Strings(20_000, 8), Strings(1, 3 << 20), Strings(20_000, 8) };
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < files.Length; i++)
            {
                inputs.Add(await Input($"{(char)('a' + i)}.parquet", files[i], storage: hooks));
            }
            var bin = Bin(table, inputs, 8L << 20);
            hooks.ClearRequests();

            var result = await Run(bin, new JobMemory(managedCap: PrefixBreakingCap(bin, files.Take(3).Select(x => x.Item1).ToArray())), hooks);

            Assert.Equal(new[] { 0, 1 }, result.Copied.Select(x => x.Input));
            Assert.Equal((2, CompactionReason.ManagedBound, false), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
            Assert.DoesNotContain(hooks.Requests, x => x.StartsWith("OpenRead") && x.EndsWith("/t/d.parquet"));
        }

        [Fact]
        public async Task AFiftyThousandElementSynthesisIsCopied()
        {
            var element = new StructType(new List<Field>() { new Field("a", Int32Type.Default, true) });
            var schema = new Schema.Builder().Field(new Field("l", new ListType(new Field("element", element, true)), true)).Build();
            var wide = Table(Enumerable.Range(0, 16).Select(x => Field($"w{x}", new Delta.StringType())).ToArray());
            var table = Table(Field("l", new Delta.ArrayType() { ElementType = Table(Field("a", new Delta.IntegerType()), Field("s", wide)), ContainsNull = true }));
            var inputs = new[] { await Input("a.parquet", Lists(schema, 50_000)), await Input("b.parquet", Lists(schema, 50_000)) };

            var result = await Run(Bin(table, inputs, 8L << 20));

            Assert.Equal(CompactionStatus.Copied, result.Status);
            Assert.Equal(new long[] { 1, 1 }, result.Copied.Select(x => x.Rows));
        }

        [Fact]
        public async Task AnInputWhoseFloorBreaksThePrefixIsNotMemoizedAndStopsTheFetch()
        {
            var hooks = HookFileStorage.Wrap(Files.Of.InternalMemory($"./cc-{Guid.NewGuid():N}"));
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var files = new[] { Strings(1, 3 << 20), Strings(20_000, 8), Strings(200_000, 8, group: 10_000), Strings(20_000, 8) };
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < files.Length; i++)
            {
                inputs.Add(await Input($"{(char)('a' + i)}.parquet", files[i], storage: hooks));
            }
            var bin = Bin(table, inputs, 8L << 20);
            hooks.ClearRequests();

            var result = await Run(bin, new JobMemory(managedCap: FloorPrefixBreakingCap(bin, files.Take(3).Select(x => x.Item1).ToArray())), hooks);

            Assert.Equal(new[] { 0, 1 }, result.Copied.Select(x => x.Input));
            Assert.Equal((2, CompactionReason.ManagedBound, false), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
            Assert.DoesNotContain(hooks.Requests, x => x.StartsWith("OpenRead") && x.EndsWith("/t/d.parquet"));
        }

        [Fact]
        public async Task AVectorThatHitsTheDiskCapFreezesThePrefix()
        {
            var remote = Files.Of.InternalMemory($"./cc-{Guid.NewGuid():N}");
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var a = await Input("a.parquet", Strings(2_000, 12), storage: remote);
            var b = await Input("b.parquet", Strings(2_000, 12, salt: 1), storage: remote);
            var c = await Input("c.parquet", Strings(2_000, 12, salt: 2), new long[] { 0, 10 }, storage: remote, padding: 2 << 20);

            var result = await Run(Bin(table, new[] { a, b, c }, 1L << 20), storage: remote, diskCap: a.Size + b.Size + (3L << 19));

            Assert.Equal(new[] { 0, 1 }, result.Copied.Select(x => x.Input));
            Assert.Equal((2, CompactionReason.ManagedBound, false), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
        }

        [Fact]
        public async Task RejectedDownloadsUseUpTheVectorAllowance()
        {
            var hooks = HookFileStorage.Wrap(Files.Of.InternalMemory($"./cc-{Guid.NewGuid():N}"));
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 3; i++)
            {
                inputs.Add(await Input($"{i}.parquet", Strings(10, 4, salt: i), new long[] { 1 }, storage: hooks, padding: 256));
            }
            hooks.ClearRequests();

            var result = await Run(Bin(table, inputs, 8L << 20), storage: hooks, limits: new CompactionLimits() { DeletionVectorBytes = 128 });

            Assert.Equal(CompactionStatus.Declined, result.Status);
            Assert.Equal(new[] { true, false, false }, result.Declines.Select(x => x.Alone));
            Assert.Single(hooks.Requests, x => x.StartsWith("OpenRead") && x.EndsWith(".bin"));
            Assert.Equal(128L, result.BytesRead);
        }

        [Theory]
        [InlineData("bitmaps")]
        [InlineData("containers")]
        [InlineData("checksum")]
        [InlineData("cardinality")]
        public async Task AMalformedVectorDeclinesItsInputAlone(string fault)
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var bad = await Input("c.parquet", Strings(100, 4, salt: 2), new long[] { 1, 2 });
            var vector = Path.Join(_root, "store", "t", "c.parquet.bin");
            var bytes = File.ReadAllBytes(vector);
            bytes[1 + 8] = fault == "bitmaps" ? (byte)2 : bytes[1 + 8];
            bytes[1 + 24] = fault == "containers" ? (byte)2 : bytes[1 + 24];
            var size = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(1));
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(5 + size), fault == "containers" ? Crc32.HashToUInt32(bytes.AsSpan(5, size)) : BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(5 + size)));
            bytes[^1] ^= fault == "checksum" ? (byte)1 : (byte)0;
            File.WriteAllBytes(vector, bytes);
            bad.DeletionVector!.Cardinality = fault == "cardinality" ? 3 : 2;
            var inputs = new[] { await Input("a.parquet", Strings(100, 4)), await Input("b.parquet", Strings(100, 4, salt: 1)), bad };

            var result = await Run(Bin(table, inputs, 8L << 20));

            Assert.Equal(CompactionStatus.Copied, result.Status);
            Assert.Equal((2, CompactionReason.Malformed, true), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
            Assert.True(fault != "containers" || result.Declines.Single().Detail.Contains("header past", StringComparison.Ordinal), result.Declines.Single().Detail);
            Assert.True(fault != "bitmaps" || result.Declines.Single().Detail.Contains("malformed frame", StringComparison.Ordinal), result.Declines.Single().Detail);
        }

        [Fact]
        public async Task AVectorPastItsPayloadDeclinesItsInputAlone()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var bad = await Input("c.parquet", Strings(20_000, 4, salt: 2), new long[] { 5 });
            var frame = DeletionVectorWriter.Frame(RoaringBitmapArray.Create(new long[] { 5 }));
            var size = BinaryPrimitives.ReadInt32BigEndian(frame) - 2;
            var bytes = new byte[1 + 4 + size + 4];
            bytes[0] = 1;
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1), size);
            frame.AsSpan(4, size).CopyTo(bytes.AsSpan(5));
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(5 + size), Crc32.HashToUInt32(bytes.AsSpan(5, size)));
            File.WriteAllBytes(Path.Join(_root, "store", "t", "c.parquet.bin"), bytes);
            bad.DeletionVector!.SizeInBytes = size;
            var inputs = new[] { await Input("a.parquet", Strings(100, 4)), await Input("b.parquet", Strings(100, 4, salt: 1)), bad };

            var result = await Run(Bin(table, inputs, 8L << 20));

            Assert.Equal(new[] { 0, 1 }, result.Copied.Select(x => x.Input));
            Assert.Equal((2, CompactionReason.Malformed, true), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
        }

        [Fact]
        public async Task AnInexactSurvivorDeclinesItsInputAlone()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("t1", new Delta.TimestampType()), Field("t2", new Delta.TimestampType()));
            var inputs = new[]
            {
                await Input("a.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 0)),
                await Input("b.parquet", Arrow(100, TimeUnit.Millisecond, TimeUnit.Millisecond, 100)),
                await Input("survivor.parquet", Nanos(100, 5)),
                await Input("deleted.parquet", Nanos(100, 7), new long[] { 7 })
            };

            var result = await Run(Bin(table, inputs, 8L << 20));

            Assert.Equal(new[] { 0, 1, 3 }, result.Copied.Select(x => x.Input));
            Assert.Equal((2, CompactionReason.Precision, true), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
        }

        [Fact]
        public async Task LocalPublishMovesStraightToTheDataName()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12, salt: 1)) };
            var staged = new List<string>();

            var result = await Run(Bin(table, inputs, 8L << 20), fault: x => staged.AddRange(x == "sync" ? Directory.GetFiles(Path.Join(_root, "store", "t")).Select(y => Path.GetFileName(y)) : Enumerable.Empty<string>()));

            Assert.Equal(new[] { "a.parquet", "b.parquet", result.Output!.Path }.Order(StringComparer.Ordinal), staged.Order(StringComparer.Ordinal));
        }

        [Fact]
        public async Task AFailedUploadCheckDeletesTheAttempt()
        {
            var hooks = HookFileStorage.Wrap(Files.Of.InternalMemory($"./cc-{Guid.NewGuid():N}"));
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12), storage: hooks), await Input("b.parquet", Strings(2_000, 12, salt: 1), storage: hooks) };
            hooks.Before = (verb, path) => verb == "Stat" && path.Full.Contains("part-00000-", StringComparison.Ordinal) ? Task.FromException(new IOException("stat")) : Task.CompletedTask;

            await Assert.ThrowsAsync<IOException>(() => Run(Bin(table, inputs, 8L << 20), storage: hooks));

            Assert.DoesNotContain(await hooks.Inner.Ls("/t/"), x => x.Name.StartsWith("part-00000-", StringComparison.Ordinal));
        }

        [Fact]
        public async Task ACancelledCopyPublishesNothing()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12, salt: 1)) };
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(Bin(table, inputs, 8L << 20), cancellationToken: cancellation.Token));

            Assert.Equal(2, Directory.GetFiles(Path.Join(_root, "store", "t")).Length);
        }

        [Fact]
        public async Task DiskIsReservedBeforeTheWriterTakesIt()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(500, 1_750, random: true)), await Input("b.parquet", Strings(500, 1_750, random: true, salt: 1)), await Input("c.parquet", Strings(500, 1_750, random: true, salt: 2)) };
            var bin = Bin(table, inputs, 1L << 20, 0.5).ToBin();
            long written = 0;

            var error = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => Run(Bin(table, inputs, 1L << 20, 0.5), diskCap: bin.TargetBytes + bin.Overshoot,
                fault: x => written = x == "close" ? Directory.GetFiles(_root, "*.zstd.parquet", SearchOption.AllDirectories).Sum(y => new FileInfo(y).Length) : written));

            Assert.Equal(MaintenanceReason.DiskCap, error.Reason);
            Assert.InRange(written, 0, bin.TargetBytes + bin.Overshoot);
        }

        [Fact]
        public async Task AWriterFailureDeclinesNoInput()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(10_000, 125, random: true)), await Input("b.parquet", Strings(10_000, 125, random: true, salt: 1)) };

            var result = await Run(Bin(table, inputs, 1L << 20), fault: x =>
            {
                if (x == "flush")
                {
                    throw new InvalidDataException("the writer failed");
                }
            });

            Assert.Equal(CompactionStatus.Abandoned, result.Status);
            Assert.Empty(result.Declines);
        }

        [Fact]
        public async Task AnInputChangedSinceAdmissionIsNotMemoized()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12, salt: 1)) };
            var copies = 0;

            var result = await Run(Bin(table, inputs, 8L << 20), fault: x =>
            {
                if (x == "copy" && ++copies == 2)
                {
                    File.AppendAllText(Path.Join(_root, "store", "t", "b.parquet"), "changed");
                }
            });

            Assert.Equal(CompactionStatus.Abandoned, result.Status);
            Assert.Empty(result.Declines);
        }

        [Fact]
        public async Task AScratchWithoutRoomForTheOutputReadsNothing()
        {
            var hooks = HookFileStorage.Wrap(Files.Of.InternalMemory($"./cc-{Guid.NewGuid():N}"));
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12), storage: hooks), await Input("b.parquet", Strings(2_000, 12, salt: 1), storage: hooks) };
            var bin = Bin(table, inputs, 8L << 20).ToBin();
            hooks.ClearRequests();

            var error = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => Run(Bin(table, inputs, 8L << 20), storage: hooks, diskCap: bin.TargetBytes + bin.Overshoot - 1));

            Assert.Equal(MaintenanceReason.DiskCap, error.Reason);
            Assert.Empty(hooks.Requests);
        }

        [Fact]
        public async Task AStopAfterOneInputLearnsTheExpansion()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 125, random: true)), await Input("b.parquet", Strings(2_000, 125, random: true, salt: 1)) };

            var result = await Run(Bin(table, inputs, 1L << 20, 50));

            Assert.Equal((CompactionStatus.Abandoned, 1), (result.Status, result.Copied.Count));
            Assert.True(result.Expansion > 0);
            Assert.Equal(result.BytesWritten, result.BytesWasted);
        }

        [Fact]
        public async Task ACorruptPageDeclinesItsInputAlone()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var (bytes, rows) = Strings(2_000, 125, random: true, salt: 2, compression: Compression.Zstd);
            for (int i = 0; i < 64; i++)
            {
                bytes[bytes.Length / 2 + i] ^= 0x5A;
            }
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12, salt: 1)), await Input("c.parquet", (bytes, rows)) };

            var result = await Run(Bin(table, inputs, 8L << 20));

            Assert.Equal(CompactionStatus.Abandoned, result.Status);
            Assert.Equal((2, CompactionReason.Malformed, true), (result.Declines.Single().Input, result.Declines.Single().Reason, result.Declines.Single().Alone));
        }

        [Fact]
        public async Task TheLastFlushAndCloseHoldTheWritersNativeTerms()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12)) };
            var memory = new JobMemory();
            long atClose = -1;

            var result = await Run(Bin(table, inputs, 8L << 20), memory, fault: x => atClose = x == "close" ? memory.Native : atClose);

            Assert.Equal(CompactionStatus.Copied, result.Status);
            Assert.True(atClose >= NativeConstants.WriterFixed + NativeConstants.WriterCodec, $"{atClose} native bytes reserved at close");
        }

        [Fact]
        public async Task ASparseListCountsOnlyItsOwnLevels()
        {
            var schema = new Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Field(new Field("dense", new ListType(new Field("element", BooleanType.Default, true)), true)).Field(new Field("sparse", new ListType(new Field("element", Int32Type.Default, true)), true)).Build();
            var table = Table(Field("id", new Delta.LongType()), Field("dense", new Delta.ArrayType() { ElementType = new Delta.BooleanType(), ContainsNull = true }), Field("sparse", new Delta.ArrayType() { ElementType = new Delta.IntegerType(), ContainsNull = true }));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 4; i++)
            {
                inputs.Add(await Input($"{i}.parquet", MixedLists(schema, 30_000, i)));
            }

            var result = await Run(Bin(table, inputs, 32L << 20));
            using var reader = new ParquetFileReader(Path.Join(_root, "store", "t", result.Output!.Path));

            Assert.Equal(4, result.Copied.Count);
            Assert.InRange(reader.FileMetaData.NumRowGroups, 1, 2);
        }

        [Fact]
        public async Task LargeInputGroupsStartTheirOwnOutputGroups()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 4; i++)
            {
                inputs.Add(await Input($"{i}.parquet", Strings(30_000, 200, random: true, salt: i, compression: Compression.Snappy)));
            }

            var result = await Run(Bin(table, inputs, 128L << 20));
            using var reader = new ParquetFileReader(Path.Join(_root, "store", "t", result.Output!.Path));

            Assert.Equal(4, result.Copied.Count);
            Assert.All(Enumerable.Range(0, reader.FileMetaData.NumRowGroups), g => Assert.InRange(reader.RowGroup(g).MetaData.TotalByteSize, 1, 9L << 20));
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        public async Task DifferentlySizedStringRowsReadmitAtTheInputsBatchFloor(int target)
        {
            var table = Table(Enumerable.Range(0, 6).Select(i => Field($"s{i}", new Delta.StringType())).ToArray());
            var codec = new DeltaParquetCodec(Compression.Snappy, ".snappy.parquet");
            var inputs = new[]
            {
                await Input("narrow.parquet", SixStrings(40_000, 28, 1)),
                await Input("wide.parquet", SixStrings(20_000, 120, 2))
            };
            var floor = inputs.Min(x => InputSummary.BatchRows(Readmit(Path.Join(_root, "store", "t", x.Path), table, (long)target << 20, Compression.Snappy).Candidate));

            var result = await Run(Bin(table, inputs, (long)target << 20) with { Codec = codec });
            var output = Readmit(Path.Join(_root, "store", "t", result.Output!.Path), table, (long)target << 20, Compression.Snappy);

            Assert.Equal(2, result.Copied.Count);
            Assert.Null(output.Reason);
            Assert.InRange(InputSummary.BatchRows(output.Candidate), Math.Min(1_024, floor), 65_536);
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        public async Task DifferentlySizedGroupsReadmitAtTheInputsBatchFloor(int target)
        {
            var table = Table(Enumerable.Range(0, 6).Select(i => Field($"s{i}", new Delta.StringType())).ToArray());
            var inputs = new[]
            {
                await Input("narrow.parquet", SixStrings(40_000, 28, 1)),
                await Input("mixed.parquet", TwoWidthStrings(20_000, 120, 100_000, 1, 20_000, 2))
            };
            var batches = inputs.Select(x => InputSummary.BatchRows(Readmit(Path.Join(_root, "store", "t", x.Path), table, (long)target << 20, Compression.Snappy).Candidate)).ToList();

            var result = await Run(Bin(table, inputs, (long)target << 20) with { Codec = new DeltaParquetCodec(Compression.Snappy, ".snappy.parquet") });
            var path = Path.Join(_root, "store", "t", result.Output!.Path);
            var output = Readmit(path, table, (long)target << 20, Compression.Snappy);
            using var reader = new ParquetFileReader(path);

            Assert.All(batches, x => Assert.Equal(65_536L, x));
            Assert.Equal(2, result.Copied.Count);
            Assert.Equal(160_000L, reader.FileMetaData.NumRows);
            Assert.Null(output.Reason);
            Assert.InRange(InputSummary.BatchRows(output.Candidate), 1_024, 65_536);
        }

        [Theory]
        [InlineData(3_072, 12_288, 8_192, false)]
        [InlineData(4_096, 8_192, 8_192, false)]
        [InlineData(4_096, 9_216, 8_192, true)]
        [InlineData(8_192, 5_120, 8_192, true)]
        [InlineData(1_024, 20_480, 8_192, false)]
        [InlineData(1_536, 2_048, 2_048, false)]
        [InlineData(1_536, 11_264, 2_048, true)]
        public void AHalfFullGroupPassingTheCeilingStartsTheNext(long open, long expected, long flush, bool starts)
        {
            Assert.Equal(starts, CompactionCopy.StartsNextGroup(open << 10, expected << 10, flush << 10));
        }

        [Theory]
        [InlineData(4_000, 1_000, 100, 100, 100_000, 10, 40)]
        [InlineData(4_000, 1_000, 2_000, 100, 100_000, 10, 200)]
        [InlineData(4_000, 1_000, 2_000, 100, 30, 10, 30)]
        [InlineData(4_000, 0, 0, 0, 100_000, 10, 40_000)]
        [InlineData(4_000, 3_000, 0, 1, 1L << 40, 65_536, 87_381)]
        [InlineData(4_000, 1_000, 1_050, 100, 100_000, 10, 105)]
        public void ABatchIsExpectedToGrowLikeItsGroupOrTheOpenGroup(long open, int written, long groupBytes, long groupRows, long bound, long rows, long expected)
        {
            Assert.Equal(expected, CompactionCopy.ExpectedGrowth(open, written, groupBytes, groupRows, bound, rows));
        }

        [Fact]
        public async Task PartialBatchesAreJudgedByTheirRowsNotTheirBound()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 2; i++)
            {
                inputs.Add(await Input($"{i}.parquet", Strings(150_000, 50, random: true, salt: i, compression: Compression.Snappy)));
            }

            var result = await Run(Bin(table, inputs, 32L << 20));
            using var reader = new ParquetFileReader(Path.Join(_root, "store", "t", result.Output!.Path));

            Assert.Equal(2, result.Copied.Count);
            Assert.Equal(2, reader.FileMetaData.NumRowGroups);
        }

        [Fact]
        public async Task SmallerInputGroupsShareAnOutputGroup()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 4; i++)
            {
                inputs.Add(await Input($"{i}.parquet", Strings(24_000, 200, random: true, salt: i, compression: Compression.Snappy)));
            }

            var result = await Run(Bin(table, inputs, 32L << 20));
            using var reader = new ParquetFileReader(Path.Join(_root, "store", "t", result.Output!.Path));

            Assert.Equal(4, result.Copied.Count);
            Assert.Equal(2, reader.FileMetaData.NumRowGroups);
        }

        [Fact]
        public async Task AWideInputAfterANarrowOneStartsItsOwnGroup()
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new List<CompactionInput>
            {
                await Input("0.parquet", Strings(300_000, 12, random: true, salt: 0, compression: Compression.Snappy)),
                await Input("1.parquet", Strings(20_000, 400, random: true, salt: 1, compression: Compression.Snappy))
            };

            var result = await Run(Bin(table, inputs, 128L << 20));
            using var reader = new ParquetFileReader(Path.Join(_root, "store", "t", result.Output!.Path));

            Assert.Equal(2, result.Copied.Count);
            Assert.Equal(2, reader.FileMetaData.NumRowGroups);
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        public async Task AWideGroupAfterANarrowOneInTheSameInputStartsItsOwnGroup(int target)
        {
            var table = Table(Enumerable.Range(0, 6).Select(i => Field($"s{i}", new Delta.StringType())).ToArray());
            var codec = new DeltaParquetCodec(Compression.Snappy, ".snappy.parquet");
            var inputs = new[]
            {
                await Input("skewed.parquet", TwoWidthStrings(105_000, 8, 18_000, 120, 105_000, 1)),
                await Input("small.parquet", SixStrings(1_000, 28, 2))
            };
            var floor = inputs.Min(x => InputSummary.BatchRows(Readmit(Path.Join(_root, "store", "t", x.Path), table, (long)target << 20, Compression.Snappy).Candidate));

            var result = await Run(Bin(table, inputs, (long)target << 20) with { Codec = codec });
            var path = Path.Join(_root, "store", "t", result.Output!.Path);
            var output = Readmit(path, table, (long)target << 20, Compression.Snappy);
            using var reader = new ParquetFileReader(path);

            Assert.InRange(floor, 16_384, 65_536);
            Assert.Equal(2, result.Copied.Count);
            Assert.Equal(3, reader.FileMetaData.NumRowGroups);
            Assert.Null(output.Reason);
            Assert.InRange(InputSummary.BatchRows(output.Candidate), Math.Min(1_024, floor), 65_536);
        }

        [Theory]
        [InlineData(2, 1)]
        [InlineData(8, 2)]
        [InlineData(32, 8)]
        [InlineData(128, 8)]
        public void TheFlushSizeStopsAtEightMebibytes(int target, int flush)
        {
            Assert.Equal((long)flush << 20, CompactionBin.FlushBytesFor((long)target << 20));
        }

        [Fact]
        public async Task DenseListOutputsAreAdmittedAgain()
        {
            var table = Table(Field("l", new Delta.ArrayType() { ElementType = new Delta.ByteType(), ContainsNull = true }));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 10; i++)
            {
                inputs.Add(await Input($"{i}.parquet", ByteLists(20_000, 25)));
            }

            var result = await Run(Bin(table, inputs, 16L << 20));
            var output = Path.Join(_root, "store", "t", result.Output!.Path);
            var admission = Readmit(output, table, 16L << 20);
            var cap = DataFileAdmission.LevelCaps(inputs.Select(x => Summarize(Path.Join(_root, "store", "t", x.Path), table)), new OutputShape(table), Settings(16L << 20))[0];
            using var reader = new ParquetFileReader(output);

            Assert.Equal(10, result.Copied.Count);
            Assert.All(Enumerable.Range(0, reader.FileMetaData.NumRowGroups), g => Assert.InRange(reader.RowGroup(g).MetaData.GetColumnChunkMetaData(0).NumValues, 1, cap));
            Assert.Null(admission.Reason);
            Assert.True(InputSummary.BatchRows(admission.Candidate) >= 1_024);
        }

        [Theory]
        [InlineData(1_179_652.25, true)]
        [InlineData(1_179_652.75, false)]
        public async Task AnOddOvershootKeepsItsFractionalHalf(double projectedBytes, bool copiesBoth)
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(100, 4)), await Input("b.parquet", Strings(100, 4, salt: 1)) };
            var expansion = projectedBytes / inputs.Sum(x => x.Size);

            var result = await Run(Bin(table, inputs, (1L << 20) + 4, expansion));

            Assert.Equal(copiesBoth ? 2 : 1, result.Copied.Count);
            Assert.Equal(copiesBoth ? CompactionStatus.Copied : CompactionStatus.Abandoned, result.Status);
            if (copiesBoth)
            {
                Assert.Equal(200, await ReadCount(Path.Join(_root, "store", "t", result.Output!.Path)));
            }
            else
            {
                Assert.Null(result.Output);
                Assert.Equal(2, Directory.GetFiles(Path.Join(_root, "store", "t")).Length);
            }
        }

        [Theory]
        [InlineData(2.0, "stop")]
        [InlineData(0.5, "cap")]
        public async Task OutputsStopEarlyOrAbandonOverTheCap(double expansion, string expected)
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new List<CompactionInput>();
            for (int i = 0; i < 3; i++)
            {
                inputs.Add(await Input($"{i}.parquet", Strings(expected == "stop" ? 2_000 : 400, expected == "stop" ? 125 : 1_750, random: true, salt: i)));
            }

            var result = await Run(Bin(table, inputs, 1L << 20, expansion));

            if (expected == "stop")
            {
                Assert.Equal((CompactionStatus.Copied, 2), (result.Status, result.Copied.Count));
                Assert.Equal(4_000, await ReadCount(Path.Join(_root, "store", "t", result.Output!.Path)));
            }
            else
            {
                Assert.Equal((CompactionStatus.Abandoned, 3), (result.Status, result.Copied.Count));
                Assert.Null(result.Output);
                Assert.Equal(3, Directory.GetFiles(Path.Join(_root, "store", "t")).Length);
            }
        }

        [Theory]
        [InlineData("copy")]
        [InlineData("close")]
        [InlineData("move")]
        [InlineData("sync")]
        public async Task AFaultAnywhereLeavesNothingHeld(string point)
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12)) };

            await Assert.ThrowsAsync<IOException>(() => Run(Bin(table, inputs, 8L << 20), fault: x =>
            {
                if (x == point)
                {
                    throw new IOException(x);
                }
            }));

            Assert.Equal(2, Directory.GetFiles(Path.Join(_root, "store", "t")).Length);
        }

        [Theory]
        [InlineData(Compression.Uncompressed, ".parquet")]
        [InlineData(Compression.Snappy, ".snappy.parquet")]
        [InlineData(Compression.Gzip, ".gz.parquet")]
        [InlineData(Compression.Lz4Hadoop, ".lz4.parquet")]
        [InlineData(Compression.Lz4, ".lz4raw.parquet")]
        [InlineData(Compression.Zstd, ".zstd.parquet")]
        public async Task OutputsAreAdmittedAgain(Compression compression, string extension)
        {
            var table = Table(Field("id", new Delta.LongType()), Field("s", new Delta.StringType()));
            var inputs = new[] { await Input("a.parquet", Strings(2_000, 12)), await Input("b.parquet", Strings(2_000, 12)) };

            var result = await Run(Bin(table, inputs, 8L << 20) with { Codec = new DeltaParquetCodec(compression, extension) });

            Assert.True(InputSummary.BatchRows(Readmit(Path.Join(_root, "store", "t", result.Output!.Path), table, 8L << 20, compression).Candidate) >= 1_024);
        }

        private async Task<CompactionResult> Run(CompactionBinValue bin, JobMemory? memory = null, IFileStorage? storage = null, Action<string>? fault = null, CompactionLimits? limits = null, long diskCap = DiskLedger.DefaultCap, CancellationToken cancellationToken = default)
        {
            var directory = _owners.Add(new ScratchDirectory(Path.Join(_root, ScratchOwners.ShortId()), diskCap));
            memory ??= new JobMemory();
            try
            {
                using var scratch = directory.CreateJob();
                var context = new CompactionContext() { Storage = storage ?? _local, Table = "t", Scratch = scratch, Memory = memory, Clock = new FakeTimeProvider(s_now), FaultForTests = fault, Limits = limits ?? CompactionLimits.Default };
                try
                {
                    return await CompactionCopy.RunAsync(context, bin.ToBin(), cancellationToken);
                }
                finally
                {
                    Assert.Empty(scratch.Files);
                    Assert.Equal(0L, directory.Ledger.Reserved);
                }
            }
            finally
            {
                Assert.Equal((0L, 0L), (memory.Managed, memory.Native));
                _owners.Release(directory);
            }
        }

        // A managed cap the third input meets alone, but its floor not beside the first two
        private long FloorPrefixBreakingCap(CompactionBinValue bin, byte[][] files)
        {
            var output = new OutputShape(bin.Table);
            var settings = new AdmissionSettings(bin.ToBin().FlushBytes, Compression.Zstd);
            var summaries = new List<InputSummary>();
            foreach (var bytes in files)
            {
                var path = Path.Join(_root, $"{Guid.NewGuid():N}.parquet");
                File.WriteAllBytes(path, bytes);
                var summary = Summarize(path, bin.Table);
                summary.Candidate = DataFileAdmission.Admit(summaries, summary, output, settings).Candidate;
                summaries.Add(summary);
            }
            var last = InputSummary.Candidates - 1;
            summaries[2].Candidate = last;
            var shared = DataFileAdmission.Freeze(summaries, output, settings);
            return DataFileAdmission.Terms(summaries[2], last, shared, summaries, output, settings).Managed - 1;
        }

        // A managed cap the first input fits beside the second, but not beside the third
        private long PrefixBreakingCap(CompactionBinValue bin, byte[][] files)
        {
            var output = new OutputShape(bin.Table);
            var target = bin.ToBin();
            var settings = new AdmissionSettings(target.FlushBytes, Compression.Zstd);
            var summaries = new List<InputSummary>();
            foreach (var bytes in files)
            {
                var path = Path.Join(_root, $"{Guid.NewGuid():N}.parquet");
                File.WriteAllBytes(path, bytes);
                var memory = new JobMemory();
                var (scan, plan) = DataFileAdmission.Preflight(path, bin.Table, false, memory);
                var summary = DataFileAdmission.Summarize(scan, plan, output);
                memory.Release(scan.ChargedBytes);
                summary.Candidate = DataFileAdmission.Admit(summaries, summary, output, settings).Candidate;
                summaries.Add(summary);
            }
            var shared = DataFileAdmission.Freeze(summaries, output, settings);
            return DataFileAdmission.Terms(summaries[0], summaries[0].Candidate, shared, summaries, output, settings).Managed - 1;
        }

        private static Admission Readmit(string path, Delta.StructType table, long target, Compression compression = Compression.Zstd)
        {
            return DataFileAdmission.Admit(System.Array.Empty<InputSummary>(), Summarize(path, table), new OutputShape(table), Settings(target, compression));
        }

        private static InputSummary Summarize(string path, Delta.StructType table)
        {
            var memory = new JobMemory();
            var (scan, plan) = DataFileAdmission.Preflight(path, table, false, memory);
            var summary = DataFileAdmission.Summarize(scan, plan, new OutputShape(table));
            memory.Release(scan.ChargedBytes);
            return summary;
        }

        private static AdmissionSettings Settings(long target, Compression compression = Compression.Zstd) => new AdmissionSettings(CompactionBin.FlushBytesFor(target), compression);

        private async Task<CompactionInput> Input(string name, (byte[] Bytes, long Rows) file, long[]? deleted = null, IFileStorage? storage = null, int padding = 0)
        {
            storage ??= _local;
            await using (var stream = await storage.OpenWrite($"/t/{name}"))
            {
                await stream.WriteAsync(file.Bytes);
            }
            DeletionVector? vector = null;
            if (deleted != null)
            {
                var frame = DeletionVectorWriter.Frame(RoaringBitmapArray.Create(deleted));
                await using (var stream = await storage.OpenWrite($"/t/{name}.bin"))
                {
                    stream.WriteByte(1);
                    await stream.WriteAsync(frame);
                    await stream.WriteAsync(new byte[Math.Max(0, padding - 1 - frame.Length)]);
                }
                vector = new DeletionVector() { StorageType = "p", PathOrInlineDv = $"{name}.bin", Offset = 1, SizeInBytes = frame.Length - DeletionVectorWriter.FrameOverhead, Cardinality = deleted.Length };
            }
            return new CompactionInput(name, file.Bytes.Length, file.Rows, vector);
        }

        // Nanosecond times, one row a tick past its millisecond
        private static (byte[], long) Nanos(int rows, int inexact)
        {
            var type = new TimestampType(TimeUnit.Nanosecond, "UTC");
            var schema = new Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Field(new Field("t1", type, true)).Field(new Field("t2", type, true)).Build();
            var times = Enumerable.Range(0, rows).Select(x => DateTimeOffset.UnixEpoch.AddMilliseconds(x).AddTicks(x == inexact ? 1 : 0)).ToList();
            var columns = new IArrowArray[]
            {
                new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => (long)x)).Build(),
                new TimestampArray.Builder(type).AppendRange(times).Build(),
                new TimestampArray.Builder(type).AppendRange(times).Build()
            };
            return (Write(schema, columns, rows, b => b), rows);
        }

        // ids from start, two timestamps in the given units, 30,000 rows a group
        private static (byte[], long) Arrow(int rows, TimeUnit first, TimeUnit second, long start, Compression compression = Compression.Zstd)
        {
            var schema = new Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Field(new Field("t1", new TimestampType(first, "UTC"), true)).Field(new Field("t2", new TimestampType(second, "UTC"), true)).Build();
            var times = Enumerable.Range(0, rows).Select(x => DateTimeOffset.UnixEpoch.AddMilliseconds(x)).ToList();
            var columns = new IArrowArray[]
            {
                new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => start + x)).Build(),
                new TimestampArray.Builder(new TimestampType(first, "UTC")).AppendRange(times).Build(),
                new TimestampArray.Builder(new TimestampType(second, "UTC")).AppendRange(times).Build()
            };
            return (Write(schema, columns, rows, b => b.Compression(compression).MaxRowGroupLength(30_000)), rows);
        }

        // INT96 and millisecond timestamps, written column by column
        private static (byte[], long) Int96(int rows, long start)
        {
            var path = Path.Join(Path.GetTempPath(), $"cc-{Guid.NewGuid():N}.parquet");
            try
            {
                using var idNode = new ParquetSharp.Schema.PrimitiveNode("id", Repetition.Optional, LogicalType.None(), PhysicalType.Int64);
                using var t1Node = new ParquetSharp.Schema.PrimitiveNode("t1", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
                using var t2Node = new ParquetSharp.Schema.PrimitiveNode("t2", Repetition.Optional, LogicalType.Timestamp(true, ParquetSharp.TimeUnit.Millis), PhysicalType.Int64);
                var fields = new ParquetSharp.Schema.Node[] { idNode, t1Node, t2Node };
                var definitions = Enumerable.Repeat((short)1, rows).ToArray();
                using (var root = new ParquetSharp.Schema.GroupNode("schema", Repetition.Required, fields))
                using (var builder = new WriterPropertiesBuilder())
                using (var properties = builder.Build())
                using (var writer = new ParquetFileWriter(path, root, properties))
                using (var group = writer.AppendRowGroup())
                {
                    using (var ids = (ColumnWriter<long>)group.NextColumn())
                    {
                        ids.WriteBatch(rows, definitions, null, Enumerable.Range(0, rows).Select(x => start + x).ToArray());
                    }
                    using (var t1 = (ColumnWriter<Int96>)group.NextColumn())
                    {
                        t1.WriteBatch(rows, definitions, null, Enumerable.Range(0, rows).Select(x => new Int96(x * 1_000, 0, 2_440_588)).ToArray());
                    }
                    using (var t2 = (ColumnWriter<long>)group.NextColumn())
                    {
                        t2.WriteBatch(rows, definitions, null, Enumerable.Range(0, rows).Select(x => (long)x).ToArray());
                    }
                    writer.Close();
                }
                return (File.ReadAllBytes(path), rows);
            }
            finally
            {
                File.Delete(path);
            }
        }

        // ids and strings of a length, random ones barely compress
        private static (byte[], long) Strings(int rows, int length, bool random = false, int group = 1 << 20, int salt = 0, Compression compression = Compression.Uncompressed)
        {
            var schema = new Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Field(new Field("s", StringType.Default, true)).Build();
            var seed = new Random(rows * 31 + length + salt);
            var columns = new IArrowArray[]
            {
                new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => (long)x)).Build(),
                new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => random ? Convert.ToBase64String(Enumerable.Range(0, length).Select(_ => (byte)seed.Next(256)).ToArray()).Substring(0, length) : new string((char)('a' + x % 26), length))).Build()
            };
            return (Write(schema, columns, rows, b => b.MaxRowGroupLength(group).Compression(compression)), rows);
        }

        // Six random strings of one width a row
        private static (byte[], long) SixStrings(int rows, int width, int salt)
        {
            var schema = new Schema(Enumerable.Range(0, 6).Select(i => new Field($"s{i}", StringType.Default, true)), null);
            var random = new Random(salt);
            var buffer = new byte[width];
            var columns = Enumerable.Range(0, 6).Select(_ => (IArrowArray)new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(_ =>
            {
                random.NextBytes(buffer);
                return Convert.ToBase64String(buffer).Substring(0, width);
            })).Build()).ToArray();
            return (Write(schema, columns, rows, b => b.Compression(Compression.Snappy).SetSizeStatisticsLevel(SizeStatisticsLevel.PageAndColumnChunk)), rows);
        }

        // Six random strings, two widths split at a row
        private static (byte[], long) TwoWidthStrings(int first, int firstWidth, int rest, int restWidth, int groupRows, int salt)
        {
            var rows = first + rest;
            var schema = new Schema(Enumerable.Range(0, 6).Select(i => new Field($"s{i}", StringType.Default, true)), null);
            var random = new Random(salt);
            var buffer = new byte[120];
            var columns = Enumerable.Range(0, 6).Select(_ => (IArrowArray)new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(row =>
            {
                random.NextBytes(buffer);
                return Convert.ToBase64String(buffer).Substring(0, row < first ? firstWidth : restWidth);
            })).Build()).ToArray();
            return (Write(schema, columns, rows, b => b.MaxRowGroupLength(groupRows).Compression(Compression.Snappy).SetSizeStatisticsLevel(SizeStatisticsLevel.PageAndColumnChunk)), rows);
        }

        // One row holding a list of elements
        private static (byte[], long) Lists(Schema schema, int elements)
        {
            var element = (StructType)((ListType)schema.FieldsList[0].DataType).ValueDataType;
            var values = new StructArray(element, elements, new[] { new Int32Array.Builder().AppendRange(Enumerable.Range(0, elements)).Build() }, ArrowBuffer.Empty, 0);
            var list = new ListArray(schema.FieldsList[0].DataType, 1, new ArrowBuffer.Builder<int>().Append(0).Append(elements).Build(), values, ArrowBuffer.Empty, 0);
            return (Write(schema, new IArrowArray[] { list }, 1, b => b), 1);
        }

        // Forty booleans and one int a row, in lists
        private static (byte[], long) MixedLists(Schema schema, int rows, int salt)
        {
            var offsets = new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, rows + 1).Select(x => x * 40)).Build();
            var dense = new ListArray(schema.FieldsList[1].DataType, rows, offsets, new BooleanArray.Builder().AppendRange(Enumerable.Range(0, rows * 40).Select(x => (x + salt) % 3 == 0)).Build(), ArrowBuffer.Empty, 0);
            var single = new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, rows + 1)).Build();
            var sparse = new ListArray(schema.FieldsList[2].DataType, rows, single, new Int32Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => x + salt)).Build(), ArrowBuffer.Empty, 0);
            var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => (long)x + salt * rows)).Build();
            return (Write(schema, new IArrowArray[] { ids, dense, sparse }, rows, b => b), rows);
        }

        // Rows of int8 lists, four values repeated
        private static (byte[], long) ByteLists(int rows, int elements)
        {
            var schema = new Schema.Builder().Field(new Field("l", new ListType(new Field("element", Int8Type.Default, true)), true)).Build();
            var values = new Int8Array.Builder().AppendRange(Enumerable.Range(0, rows * elements).Select(x => (sbyte)(x % 4))).Build();
            var offsets = new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, rows + 1).Select(x => x * elements)).Build();
            var list = new ListArray(schema.FieldsList[0].DataType, rows, offsets, values, ArrowBuffer.Empty, 0);
            return (Write(schema, new IArrowArray[] { list }, rows, b => b), rows);
        }

        private static byte[] Write(Schema schema, IArrowArray[] columns, int rows, Func<WriterPropertiesBuilder, WriterPropertiesBuilder> properties)
        {
            using var stream = new MemoryStream();
            using (var builder = new WriterPropertiesBuilder())
            using (var writerProperties = properties(builder).Build())
            using (var batch = new RecordBatch(schema, columns, rows))
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, schema, writerProperties, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            return stream.ToArray();
        }

        private static async Task<List<long>> ReadLongs(string path, int column)
        {
            var values = new List<long>();
            using var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 65_536, new FileInfo(path).Length));
            for (int g = 0; g < file.RowGroupStarts.Length; g++)
            {
                await foreach (var batch in file.ReadRowGroup(g, new[] { column }))
                {
                    using (batch)
                    {
                        values.AddRange(((PrimitiveArray<long>)batch.Column(0)).Values.ToArray());
                    }
                }
            }
            return values;
        }

        private static async Task<long> ReadCount(string path)
        {
            return (await ReadLongs(path, 0)).Count;
        }

        private static CompactionBinValue Bin(Delta.StructType table, IReadOnlyList<CompactionInput> inputs, long target, double expansion = 1)
        {
            return new CompactionBinValue(table, inputs, target, expansion, s_zstd);
        }

        // A bin's settings as a value, so tests can vary one
        private sealed record CompactionBinValue(Delta.StructType Table, IReadOnlyList<CompactionInput> Inputs, long Target, double Expansion, DeltaParquetCodec Codec)
        {
            public CompactionBin ToBin() => new CompactionBin() { Table = Table, TableName = "t", Codec = Codec, TargetBytes = Target, Expansion = Expansion, Inputs = Inputs };
        }

        private static Delta.StructType Table(params Delta.StructField[] fields) => new Delta.StructType(fields);

        private static Delta.StructField Field(string name, Internal.Delta.Schema.SchemaBaseType type) => new Delta.StructField(name, type, true, new Dictionary<string, object>());
    }
}
