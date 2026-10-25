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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Random actions and log lines for the record and encoder property tests
    internal static class RandomActions
    {
        private static readonly string[] s_strings = { "", "a", "path/part-0.parquet", "é", "😀", "x\"y", "back\\slash", "line\nbreak", "\u0001", "a\0b", "space here", "%20encoded" };

        public static string Text(Random random) => random.Next(4) == 0 ? s_strings[random.Next(s_strings.Length)] : s_strings[random.Next(s_strings.Length)] + random.Next(1000);

        private static string? NullableText(Random random) => random.Next(5) == 0 ? null : Text(random);

        private static long? NullableLong(Random random) => random.Next(4) == 0 ? null : Long(random);

        private static long Long(Random random) => random.Next(3) switch
        {
            0 => random.Next(-5, 5),
            1 => random.NextInt64(long.MinValue, long.MaxValue),
            _ => random.NextInt64(0, 1_000_000_000_000)
        };

        private static Dictionary<string, string>? Map(Random random)
        {
            if (random.Next(4) == 0)
            {
                return null;
            }
            var map = new Dictionary<string, string>();
            var count = random.Next(5);
            for (int i = 0; i < count; i++)
            {
                map[Text(random)] = NullableText(random)!;
            }
            return map;
        }

        private static List<string>? List(Random random)
        {
            if (random.Next(4) == 0)
            {
                return null;
            }
            return Enumerable.Range(0, random.Next(4)).Select(_ => NullableText(random)!).ToList();
        }

        private static DeletionVector? Vector(Random random)
        {
            if (random.Next(2) == 0)
            {
                return null;
            }
            return new DeletionVector()
            {
                StorageType = NullableText(random),
                PathOrInlineDv = NullableText(random),
                Offset = NullableLong(random),
                SizeInBytes = random.Next(int.MinValue, int.MaxValue),
                Cardinality = Long(random)
            };
        }

        public static DeltaAction Next(Random random)
        {
            switch (random.Next(6))
            {
                case 0:
                    return new DeltaAction()
                    {
                        Add = new DeltaAddAction()
                        {
                            Path = NullableText(random),
                            PartitionValues = Map(random),
                            Size = Long(random),
                            ModificationTime = Long(random),
                            DataChange = random.Next(2) == 0,
                            Statistics = NullableText(random),
                            Tags = Map(random),
                            DeletionVector = Vector(random),
                            BaseRowId = NullableLong(random),
                            DefaultRowCommitVersion = NullableLong(random),
                            ClusteringProvider = NullableText(random)
                        }
                    };
                case 1:
                    return new DeltaAction()
                    {
                        Remove = new DeltaRemoveFileAction()
                        {
                            Path = NullableText(random),
                            DeletionTimestamp = NullableLong(random),
                            DataChange = random.Next(2) == 0,
                            ExtendedFileMetadata = random.Next(2) == 0,
                            PartitionValues = Map(random),
                            Size = NullableLong(random),
                            DeletionVector = Vector(random),
                            BaseRowId = NullableLong(random),
                            DefaultRowCommitVersion = NullableLong(random)
                        }
                    };
                case 2:
                    return new DeltaAction()
                    {
                        MetaData = new DeltaMetadataAction()
                        {
                            Id = NullableText(random),
                            Name = NullableText(random),
                            Description = NullableText(random),
                            Format = random.Next(3) == 0 ? null : new DeltaMetadataFormat() { Provider = NullableText(random), Options = Map(random) },
                            SchemaString = NullableText(random),
                            PartitionColumns = List(random),
                            CreatedTime = NullableLong(random),
                            Configuration = Map(random)
                        }
                    };
                case 3:
                    return new DeltaAction()
                    {
                        Protocol = new DeltaProtocolAction()
                        {
                            MinReaderVersion = random.Next(-3, 10),
                            MinWriterVersion = random.Next(int.MinValue, int.MaxValue),
                            ReaderFeatures = List(random),
                            WriterFeatures = List(random)
                        }
                    };
                case 4:
                    return new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = Text(random), Version = Long(random), LastUpdated = NullableLong(random) } };
                default:
                    return new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = Text(random), Configuration = NullableText(random), Removed = random.Next(2) == 0 } };
            }
        }

        /// <summary>
        /// The model as JSON with maps sorted and the remove fields a checkpoint never keeps cleared, for comparisons without regard to map order.
        /// </summary>
        public static string Canonical(DeltaAction action)
        {
            static Dictionary<string, string>? Sorted(Dictionary<string, string>? map) => map?.OrderBy(x => x.Key, StringComparer.Ordinal).ToDictionary(x => x.Key, x => x.Value);
            if (action.Add != null)
            {
                action.Add.PartitionValues = Sorted(action.Add.PartitionValues);
                action.Add.Tags = Sorted(action.Add.Tags);
            }
            if (action.Remove != null)
            {
                action.Remove.PartitionValues = Sorted(action.Remove.PartitionValues);
                action.Remove.Stats = null;
                action.Remove.Tags = null;
            }
            if (action.MetaData != null)
            {
                action.MetaData.Configuration = Sorted(action.MetaData.Configuration);
                if (action.MetaData.Format != null)
                {
                    action.MetaData.Format.Options = Sorted(action.MetaData.Format.Options);
                }
            }
            return JsonSerializer.Serialize(action);
        }

        /// <summary>
        /// A log line as text, with escapes, reordered and duplicate properties, unknown properties, nulls and occasional wrong types.
        /// </summary>
        public static string Line(Random random)
        {
            var actions = new List<string>();
            var count = random.Next(1, 4);
            for (int i = 0; i < count; i++)
            {
                actions.Add(random.Next(9) switch
                {
                    0 => Property(random, "add") + Object(random, AddFields),
                    1 => Property(random, "remove") + Object(random, RemoveFields),
                    2 => Property(random, "metaData") + Object(random, MetaDataFields),
                    3 => Property(random, "protocol") + Object(random, ProtocolFields),
                    4 => Property(random, "txn") + Object(random, TxnFields),
                    5 => Property(random, "domainMetadata") + Object(random, DomainFields),
                    6 => Property(random, "cdc") + Object(random, CdcFields),
                    7 => Property(random, "commitInfo") + Object(random, CommitInfoFields),
                    _ => Property(random, "unknown" + random.Next(3)) + AnyValue(random, 3)
                });
            }
            if (random.Next(10) == 0)
            {
                actions.Add(Property(random, "add") + "null");
            }
            return "{" + string.Join("," + Space(random), actions) + "}";
        }

        private static string Space(Random random) => random.Next(5) == 0 ? " \t" : "";

        private static string Property(Random random, string name)
        {
            // Escaping a property name keeps its meaning
            var text = random.Next(8) == 0 ? "\\u" + ((int)name[0]).ToString("x4") + name.Substring(1) : name;
            return "\"" + text + "\":" + Space(random);
        }

        private static string Quoted(string value)
        {
            var builder = new StringBuilder("\"");
            for (int i = 0; i < value.Length; i++)
            {
                var c = value[i];
                if (char.IsHighSurrogate(c) && i + 1 < value.Length)
                {
                    // A surrogate pair is written raw or as two escapes, never half of each
                    builder.Append(value.Length % 2 == 0 ? $"\\u{(int)c:x4}\\u{(int)value[i + 1]:x4}" : value.Substring(i, 2));
                    i++;
                    continue;
                }
                builder.Append(c switch
                {
                    '"' => "\\\"",
                    '\\' => "\\\\",
                    '\n' => "\\n",
                    '\0' => "\\u0000",
                    '\u0001' => "\\u0001",
                    'é' => value.Length % 2 == 0 ? "\\u00e9" : "é",
                    _ => c.ToString()
                });
            }
            return builder.Append('"').ToString();
        }

        private static string StringValue(Random random) => random.Next(6) == 0 ? "null" : Quoted(Text(random));

        private static string LongValue(Random random, bool nullable) => random.Next(40) switch
        {
            0 => "1.0",
            1 => "\"12\"",
            2 => "99999999999999999999",
            3 => nullable ? "null" : "1e2",
            _ => Long(random).ToString(CultureInfo.InvariantCulture)
        };

        private static string IntValue(Random random) => random.Next(30) switch
        {
            0 => "3000000000",
            1 => "null",
            _ => random.Next(-100, 100).ToString(CultureInfo.InvariantCulture)
        };

        private static string BoolValue(Random random, bool nullable) => random.Next(30) switch
        {
            0 => nullable ? "null" : "1",
            1 => "\"true\"",
            _ => random.Next(2) == 0 ? "true" : "false"
        };

        private static string MapValue(Random random)
        {
            if (random.Next(6) == 0)
            {
                return random.Next(15) == 0 ? "[]" : "null";
            }
            var entries = new List<string>();
            var count = random.Next(5);
            for (int i = 0; i < count; i++)
            {
                // Duplicate and escaped keys follow the reader
                var key = random.Next(4) == 0 ? "k" : Text(random);
                var value = random.Next(25) == 0 ? "5" : StringValue(random);
                entries.Add(Quoted(key) + ":" + value);
            }
            if (count > 0 && random.Next(4) == 0)
            {
                entries.Add("\"\\u006b\":" + StringValue(random));
            }
            return "{" + string.Join(",", entries) + "}";
        }

        private static string ListValue(Random random)
        {
            if (random.Next(6) == 0)
            {
                return random.Next(15) == 0 ? "{}" : "null";
            }
            var items = Enumerable.Range(0, random.Next(4)).Select(_ => random.Next(30) == 0 ? "7" : StringValue(random));
            return "[" + string.Join(",", items) + "]";
        }

        private static string AnyValue(Random random, int depth)
        {
            return (depth <= 0 ? random.Next(4) : random.Next(6)) switch
            {
                0 => StringValue(random),
                1 => LongValue(random, true),
                2 => "true",
                3 => "null",
                4 => "{" + string.Join(",", Enumerable.Range(0, random.Next(3)).Select(i => Quoted("u" + i) + ":" + AnyValue(random, depth - 1))) + "}",
                _ => "[" + string.Join(",", Enumerable.Range(0, random.Next(3)).Select(_ => AnyValue(random, depth - 1))) + "]"
            };
        }

        private delegate string FieldValue(Random random);

        private static string VectorValue(Random random) => random.Next(4) == 0 ? "null" : Object(random, VectorFields);

        private static string FormatValue(Random random) => random.Next(4) == 0 ? "null" : Object(random, FormatFields);

        private static readonly (string Name, FieldValue Value)[] VectorFields =
        {
            ("storageType", StringValue), ("pathOrInlineDv", StringValue), ("offset", r => LongValue(r, true)), ("sizeInBytes", IntValue), ("cardinality", r => LongValue(r, false))
        };

        private static readonly (string Name, FieldValue Value)[] AddFields =
        {
            ("path", StringValue), ("partitionValues", MapValue), ("size", r => LongValue(r, false)), ("modificationTime", r => LongValue(r, false)),
            ("dataChange", r => BoolValue(r, false)), ("stats", StringValue), ("tags", MapValue), ("deletionVector", VectorValue),
            ("baseRowId", r => LongValue(r, true)), ("defaultRowCommitVersion", r => LongValue(r, true)), ("clusteringProvider", StringValue)
        };

        private static readonly (string Name, FieldValue Value)[] RemoveFields =
        {
            ("path", StringValue), ("deletionTimestamp", r => LongValue(r, true)), ("dataChange", r => BoolValue(r, false)), ("extendedFileMetadata", r => BoolValue(r, false)),
            ("partitionValues", MapValue), ("size", r => LongValue(r, true)), ("stats", StringValue), ("tags", MapValue), ("deletionVector", VectorValue),
            ("baseRowId", r => LongValue(r, true)), ("defaultRowCommitVersion", r => LongValue(r, true))
        };

        private static readonly (string Name, FieldValue Value)[] FormatFields = { ("provider", StringValue), ("options", MapValue) };

        private static readonly (string Name, FieldValue Value)[] MetaDataFields =
        {
            ("id", StringValue), ("name", StringValue), ("description", StringValue), ("format", FormatValue), ("schemaString", StringValue),
            ("partitionColumns", ListValue), ("createdTime", r => LongValue(r, true)), ("configuration", MapValue)
        };

        private static readonly (string Name, FieldValue Value)[] ProtocolFields =
        {
            ("minReaderVersion", IntValue), ("minWriterVersion", IntValue), ("readerFeatures", ListValue), ("writerFeatures", ListValue)
        };

        private static readonly (string Name, FieldValue Value)[] TxnFields = { ("appId", StringValue), ("version", r => LongValue(r, false)), ("lastUpdated", r => LongValue(r, true)) };

        private static readonly (string Name, FieldValue Value)[] DomainFields = { ("domain", StringValue), ("configuration", StringValue), ("removed", r => BoolValue(r, false)) };

        private static readonly (string Name, FieldValue Value)[] CdcFields =
        {
            ("path", StringValue), ("partitionValues", MapValue), ("size", r => LongValue(r, false)), ("dataChange", r => BoolValue(r, false)), ("tags", MapValue)
        };

        private static readonly (string Name, FieldValue Value)[] CommitInfoFields =
        {
            ("timestamp", r => LongValue(r, true)), ("operation", StringValue), ("flowtide.stageId", r => AnyValue(r, 1)), ("operationParameters", r => AnyValue(r, 2))
        };

        private static string Object(Random random, (string Name, FieldValue Value)[] fields)
        {
            var properties = new List<string>();
            foreach (var field in fields.OrderBy(_ => random.Next()))
            {
                if (random.Next(5) == 0)
                {
                    continue;
                }
                properties.Add(Property(random, field.Name) + field.Value(random));
                // The last occurrence of a duplicate property wins
                if (random.Next(12) == 0)
                {
                    properties.Add(Property(random, field.Name) + field.Value(random));
                }
            }
            if (random.Next(4) == 0)
            {
                properties.Insert(random.Next(properties.Count + 1), Property(random, "extra" + random.Next(3)) + AnyValue(random, 2));
            }
            return "{" + string.Join("," + Space(random), properties) + "}";
        }
    }
}
