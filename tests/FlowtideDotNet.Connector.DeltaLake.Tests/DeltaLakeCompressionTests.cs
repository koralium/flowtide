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
using ParquetSharp;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeCompressionTests
    {
        [Fact]
        public async Task NewTablesWriteZstdFiles()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(NewTablesWriteZstdFiles)}");
            await using var stream = new DeltaLakeSinkStream(nameof(NewTablesWriteZstdFiles), storage, options =>
            {
                options.CheckpointInterval = 2;
                options.WriteChangeDataOnNewTables = true;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // 2 of 10 rows is above the deletion vector limit, the file is rewritten and change data is kept
            stream.DeleteUser(stream.Users[0]);
            stream.DeleteUser(stream.Users[1]);
            await WaitForVersion(storage, "test", stream, 1);
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 2);
            await WaitForCheckpoint(storage, "test", stream, 2);

            var rewrite = await ReadCommitActions(storage, "test", 1);
            Assert.Contains(rewrite, x => x.Remove != null);
            var rewritten = rewrite.Where(x => x.Add != null).Select(x => x.Add!.Path!).ToList();
            Assert.NotEmpty(rewritten);
            var changeData = rewrite.Where(x => x.Cdc != null).Select(x => x.Cdc!.Path!).ToList();
            Assert.NotEmpty(changeData);

            var dataFiles = (await storage.Ls("/test/")).Where(x => !x.Path.IsFolder && x.Name.EndsWith(".parquet")).Select(x => x.Name).ToList();
            Assert.NotEmpty(dataFiles);
            Assert.Superset(rewritten.ToHashSet(), dataFiles.ToHashSet());
            foreach (var file in dataFiles.Concat(changeData))
            {
                Assert.EndsWith(".zstd.parquet", file);
                Assert.All(await ReadCompressions(storage, $"/test/{file}"), codec => Assert.Equal(Compression.Zstd, codec));
            }
            Assert.All(await ReadCompressions(storage, "/test/_delta_log/00000000000000000002.checkpoint.parquet"), codec => Assert.Equal(Compression.Zstd, codec));
        }

        [Theory]
        [InlineData(0, "snappy", Compression.Snappy, ".snappy.parquet")]
        [InlineData(1, "SNAPPY", Compression.Snappy, ".snappy.parquet")]
        [InlineData(2, "gzip", Compression.Gzip, ".gz.parquet")]
        [InlineData(3, "lz4", Compression.Lz4Hadoop, ".lz4.parquet")]
        [InlineData(4, "lz4_raw", Compression.Lz4, ".lz4raw.parquet")]
        [InlineData(5, "Zstd", Compression.Zstd, ".zstd.parquet")]
        [InlineData(6, "uncompressed", Compression.Uncompressed, "-0.parquet")]
        [InlineData(7, "none", Compression.Uncompressed, "-0.parquet")]
        public async Task TablePropertyChoosesTheCodec(int id, string value, Compression expected, string suffix)
        {
            var testName = $"{nameof(TablePropertyChoosesTheCodec)}_{id}";
            var (codecs, files) = await WriteToTableWithCodec(testName, value, null);

            Assert.All(codecs, codec => Assert.Equal(expected, codec));
            Assert.All(files, file => Assert.EndsWith(suffix, file));
        }

        [Fact]
        public async Task OptionOverridesTheTableProperty()
        {
            var (codecs, _) = await WriteToTableWithCodec(nameof(OptionOverridesTheTableProperty), "zstd", DeltaCompressionCodec.Uncompressed);

            Assert.All(codecs, codec => Assert.Equal(Compression.Uncompressed, codec));
        }

        [Fact]
        public async Task UnknownCodecFallsBackToZstdAndWarnsOnce()
        {
            var testName = nameof(UnknownCodecFallsBackToZstdAndWarnsOnce);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema, new Dictionary<string, string>() { ["delta.parquet.compression.codec"] = "lzo" }));
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.AddLoggerProvider(logs);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);
            stream.Generate(5);
            await WaitForVersion(storage, "test", stream, 2);

            foreach (var version in new[] { 1L, 2L })
            {
                foreach (var add in (await ReadCommitActions(storage, "test", version)).Where(x => x.Add != null))
                {
                    Assert.All(await ReadCompressions(storage, $"/test/{add.Add!.Path}"), codec => Assert.Equal(Compression.Zstd, codec));
                }
            }
            Assert.Single(logs.Warnings, x => x.Contains("lzo"));
        }

        [Fact]
        public async Task DuckDbReadsZstdFiles()
        {
            var tempPath = Path.Join(Directory.GetCurrentDirectory(), "test_duckdb_zstd_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempPath);
            try
            {
                var storage = Files.Of.LocalDisk(tempPath);
                await using var stream = new DeltaLakeSinkStream(nameof(DuckDbReadsZstdFiles), storage);
                stream.WaitForUpdateDoesNotRequireDataChange();
                stream.Generate(20);
                await stream.StartStream(UserInsert);
                await WaitForVersion(storage, "test", stream, 0);
                stream.DeleteUser(stream.Users[0]);
                await WaitForVersion(storage, "test", stream, 1);
                // On local disk a commit file exists before its bytes are written
                await SettlePublications(stream);

                var parquetFile = Directory.GetFiles(Path.Join(tempPath, "test"), "*.parquet").First();
                Assert.EndsWith(".zstd.parquet", parquetFile);

                using var conn = new DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:");
                conn.Open();
                using (var setup = conn.CreateCommand())
                {
                    var extensionsDir = Path.Join(Directory.GetCurrentDirectory(), "duckdb_extensions").Replace("\\", "/");
                    setup.CommandText = $"SET extension_directory = '{extensionsDir}'; INSTALL delta; LOAD delta;";
                    setup.ExecuteNonQuery();
                }
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT count(*) FROM delta_scan('{Path.Join(tempPath, "test").Replace("\\", "/")}')";
                var count = Convert.ToInt64(cmd.ExecuteScalar());
                Assert.Equal(stream.Users.Count, count);
            }
            finally
            {
                try { Directory.Delete(tempPath, true); } catch { }
            }
        }

        private static async Task<(List<Compression> Codecs, List<string> Files)> WriteToTableWithCodec(string testName, string codec, DeltaCompressionCodec? option)
        {
            var storage = Files.Of.InternalMemory($"./{testName}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema, new Dictionary<string, string>() { ["delta.parquet.compression.codec"] = codec }));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CompressionCodec = option);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 1);

            var files = (await ReadCommitActions(storage, "test", 1)).Where(x => x.Add != null).Select(x => x.Add!.Path!).ToList();
            Assert.NotEmpty(files);
            var codecs = new List<Compression>();
            foreach (var file in files)
            {
                codecs.AddRange(await ReadCompressions(storage, $"/test/{file}"));
            }
            Assert.NotEmpty(codecs);
            return (codecs, files.Select(NormalizeName).ToList());
        }

        // Replaces the name before the first dot, an uncompressed file then ends with "-0.parquet"
        private static string NormalizeName(string file)
        {
            return "-0" + file.Substring(file.IndexOf('.'));
        }
    }

    [CollectionDefinition(nameof(HostWriterDefaultsCollection), DisableParallelization = true)]
    public class HostWriterDefaultsCollection
    {
    }

    [Collection(nameof(HostWriterDefaultsCollection))]
    public class DeltaLakeHostWriterDefaultsTests
    {
        [Fact]
        public async Task HostCompressionLevelDoesNotBreakCodecsWithoutLevels()
        {
            var testName = nameof(HostCompressionLevelDoesNotBreakCodecsWithoutLevels);
            var previous = DefaultWriterProperties.CompressionLevel;
            DefaultWriterProperties.CompressionLevel = 5;
            DeltaParquetCompression.ResetCacheForTests();
            try
            {
                var storage = Files.Of.InternalMemory($"./{testName}");
                await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema, new Dictionary<string, string>() { ["delta.parquet.compression.codec"] = "snappy" }));
                await using var stream = new DeltaLakeSinkStream(testName, storage);
                stream.Generate(10);
                await stream.StartStream(UserInsert);
                await WaitForVersion(storage, "test", stream, 1);

                foreach (var add in (await ReadCommitActions(storage, "test", 1)).Where(x => x.Add != null))
                {
                    Assert.All(await ReadCompressions(storage, $"/test/{add.Add!.Path}"), codec => Assert.Equal(Compression.Snappy, codec));
                }
            }
            finally
            {
                DefaultWriterProperties.CompressionLevel = previous;
                DeltaParquetCompression.ResetCacheForTests();
            }
        }
    }
}
