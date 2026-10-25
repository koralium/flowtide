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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using Stowage;
using System.Text;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A version 0 checkpoint from today's writer plus JSON tail commits; data file paths are fake
    internal static class LegacyTableGenerator
    {
        private const string DvAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

        // Commit timestamps start here, one millisecond per version
        public const long BaseTimestamp = 1_760_000_000_000;

        /// <summary>
        /// Returns the protocol, metaData and add actions written into the root, in that order.
        /// Columns alternate long and string, named c0, c1, ... unless <paramref name="names"/> is given.
        /// </summary>
        public static async Task<List<DeltaAction>> Create(IFileStorage storage, string path, int files, int statsColumns, int tailCommits, bool withDvs, IReadOnlyList<string>? names = null)
        {
            var random = new Random(files * 31 + statsColumns * 7 + (withDvs ? 1 : 0));
            var schema = Schema(statsColumns, names);
            var protocol = withDvs ? ProtocolWith("deletionVectors") : ProtocolWith();
            var configuration = withDvs ? new Dictionary<string, string>() { ["delta.enableDeletionVectors"] = "true" } : new Dictionary<string, string>();
            var metadata = Metadata(schema, configuration);

            var root = new List<DeltaAction>() { protocol, metadata };
            var live = new List<DeltaAddAction>(files);
            for (int i = 0; i < files; i++)
            {
                var add = NewAdd(random, $"part-{i:D8}-{NewGuid(random)}.zstd.parquet", statsColumns, names, withDvs && i % 10 == 0);
                root.Add(new DeltaAction() { Add = add });
                live.Add(add);
            }

            var table = new DeltaTable(metadata.MetaData!, protocol.Protocol!, new List<DeltaAddAction>(live), ParseSchema(schema), 0,
                Array.Empty<DeltaRemoveFileAction>(), new Dictionary<string, DeltaTransactionAction>(), new Dictionary<string, DeltaDomainMetadataAction>(), null);
            await LegacyCheckpointWriter.WriteCheckpoint(storage, path, table);

            for (int version = 1; version <= tailCommits; version++)
            {
                var timestamp = BaseTimestamp + version;
                var actions = new List<DeltaAction>()
                {
                    new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["timestamp"] = timestamp, ["operation"] = "WRITE" } } }
                };
                var index = random.Next(live.Count);
                var old = live[index];
                actions.Add(new DeltaAction() { Remove = RemoveOf(old, timestamp) });
                // With deletion vectors, every other commit replaces the file's vector instead of the file
                var replacement = withDvs && version % 2 == 0
                    ? CopyWithVector(old, NewVector(random))
                    : NewAdd(random, $"part-t{version:D6}-{NewGuid(random)}.zstd.parquet", statsColumns, names, withDvs && version % 3 == 0);
                actions.Add(new DeltaAction() { Add = replacement });
                live[index] = replacement;
                await WriteCommit(storage, path, version, actions.ToArray());
            }
            return root;
        }

        public static string Schema(int columns, IReadOnlyList<string>? names = null)
        {
            var fields = Enumerable.Range(0, columns).Select(c => c % 2 == 0
                ? $"{{\"name\":\"{Name(c, names)}\",\"type\":\"long\",\"nullable\":true,\"metadata\":{{}}}}"
                : $"{{\"name\":\"{Name(c, names)}\",\"type\":\"string\",\"nullable\":true,\"metadata\":{{}}}}");
            return $"{{\"type\":\"struct\",\"fields\":[{string.Join(",", fields)}]}}";
        }

        private static string Name(int column, IReadOnlyList<string>? names) => names?[column] ?? $"c{column}";

        private static StructType ParseSchema(string schema)
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new TypeConverter());
            return (StructType)JsonSerializer.Deserialize<SchemaBaseType>(schema, options)!;
        }

        private static DeltaAddAction NewAdd(Random random, string path, int statsColumns, IReadOnlyList<string>? names, bool withDv)
        {
            return new DeltaAddAction()
            {
                Path = path,
                PartitionValues = new Dictionary<string, string>(),
                Size = random.Next(1_000_000, 100_000_000),
                ModificationTime = BaseTimestamp - random.Next(1_000_000),
                DataChange = true,
                Statistics = Stats(random, statsColumns, names),
                DeletionVector = withDv ? NewVector(random) : null
            };
        }

        private static string Stats(Random random, int columns, IReadOnlyList<string>? names)
        {
            var min = new StringBuilder();
            var max = new StringBuilder();
            var nulls = new StringBuilder();
            for (int c = 0; c < columns; c++)
            {
                var separator = c == 0 ? "" : ",";
                if (c % 2 == 0)
                {
                    var low = random.NextInt64(0, long.MaxValue / 2);
                    min.Append($"{separator}\"{Name(c, names)}\":{low}");
                    max.Append($"{separator}\"{Name(c, names)}\":{low + random.Next(1, 1_000_000)}");
                }
                else
                {
                    min.Append($"{separator}\"{Name(c, names)}\":\"a{random.Next():D10}\"");
                    max.Append($"{separator}\"{Name(c, names)}\":\"z{random.Next():D10}\"");
                }
                nulls.Append($"{separator}\"{Name(c, names)}\":0");
            }
            return $"{{\"numRecords\":{random.Next(1_000, 100_000)},\"minValues\":{{{min}}},\"maxValues\":{{{max}}},\"nullCount\":{{{nulls}}}}}";
        }

        private static DeletionVector NewVector(Random random)
        {
            var chars = new char[20];
            for (int i = 0; i < chars.Length; i++)
            {
                chars[i] = DvAlphabet[random.Next(DvAlphabet.Length)];
            }
            return new DeletionVector() { StorageType = "u", PathOrInlineDv = new string(chars), Offset = 1, SizeInBytes = random.Next(30, 400), Cardinality = random.Next(1, 500) };
        }

        private static DeltaAddAction CopyWithVector(DeltaAddAction add, DeletionVector vector)
        {
            return new DeltaAddAction()
            {
                Path = add.Path,
                PartitionValues = add.PartitionValues,
                Size = add.Size,
                ModificationTime = add.ModificationTime,
                DataChange = true,
                Statistics = add.Statistics,
                Tags = add.Tags,
                DeletionVector = vector
            };
        }

        private static DeltaRemoveFileAction RemoveOf(DeltaAddAction add, long timestamp)
        {
            return new DeltaRemoveFileAction()
            {
                Path = add.Path,
                DeletionTimestamp = timestamp,
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = add.PartitionValues,
                Size = add.Size,
                DeletionVector = add.DeletionVector
            };
        }

        private static Guid NewGuid(Random random)
        {
            var bytes = new byte[16];
            random.NextBytes(bytes);
            return new Guid(bytes);
        }
    }
}
