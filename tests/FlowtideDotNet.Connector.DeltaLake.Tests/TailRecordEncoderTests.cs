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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class TailRecordEncoderTests
    {
        // A record's leaf events as text: map entries sorted, as map order has no meaning, with duplicates and nulls kept apart from defaults
        internal static string RawCanonical(ReadOnlySpan<byte> record)
        {
            var visitor = new CanonicalVisitor() { Text = new StringBuilder() };
            RecordWalker.Walk(record, ref visitor);
            return visitor.Text.ToString();
        }

        private struct CanonicalVisitor : IRecordVisitor
        {
            public StringBuilder Text;
            private List<string>? _entries;
            private StringBuilder? _entry;
            private int _arity;
            private int _leaves;
            private int _remaining;
            private bool _sort;

            public void Struct(StructNode node, bool present) => Token($"{node}:{(present ? "present" : "null")}");

            public void String(Leaf leaf, bool present, ReadOnlySpan<byte> utf8) => Token(present ? "s" + Convert.ToHexString(utf8) : "null");

            public void Int64(Leaf leaf, long? value) => Token(value?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "null");

            public void Bool(Leaf leaf, bool? value) => Token(value?.ToString() ?? "null");

            public void Repeated(RepeatedNode node, int? count)
            {
                Text.Append(node).Append(':').Append(count?.ToString() ?? "null").Append(';');
                _sort = node is not (RepeatedNode.MetaPartitionColumns or RepeatedNode.ProtocolReaderFeatures or RepeatedNode.ProtocolWriterFeatures);
                _arity = _sort ? 2 : 1;
                _remaining = (count ?? 0) * _arity;
                _entries = _remaining > 0 ? new List<string>() : null;
                _entry = new StringBuilder();
                _leaves = 0;
            }

            private void Token(string token)
            {
                if (_remaining == 0)
                {
                    Text.Append(token).Append(';');
                    return;
                }
                _entry!.Append(token).Append(',');
                _remaining--;
                if (++_leaves == _arity)
                {
                    _entries!.Add(_entry.ToString());
                    _entry.Clear();
                    _leaves = 0;
                }
                if (_remaining == 0)
                {
                    if (_sort)
                    {
                        _entries!.Sort(StringComparer.Ordinal);
                    }
                    Text.Append('[').AppendJoin("|", _entries!).Append("];");
                    _entries = null;
                }
            }
        }

        // The reader's records of a line in canonical form, or null when the reader rejects it
        private static List<string>? ReaderRecords(string line)
        {
            DeltaAction? action;
            try
            {
                action = JsonSerializer.Deserialize<DeltaAction>(line);
            }
            catch (Exception)
            {
                return null;
            }
            return action == null ? null : TestRecords.FromAction(action).Select(x => RawCanonical(x)).ToList();
        }

        private static List<string>? EncoderRecords(TailRecordEncoder encoder, string line, out MaintenanceReason? declined)
        {
            var records = new List<string>();
            try
            {
                encoder.Encode(Encoding.UTF8.GetBytes(line), record => records.Add(RawCanonical(record)));
                declined = null;
                return records;
            }
            catch (CheckpointDeclinedException e)
            {
                declined = e.Reason;
                return null;
            }
        }

        private static void AssertParity(TailRecordEncoder encoder, string line)
        {
            var expected = ReaderRecords(line);
            var actual = EncoderRecords(encoder, line, out var declined);
            if (expected == null)
            {
                Assert.True(actual == null, $"The reader rejects, the encoder accepts: {line}");
                Assert.Equal(MaintenanceReason.InvalidLogLine, declined);
                return;
            }
            Assert.True(actual != null, $"The encoder declines ({declined}), the reader accepts: {line}");
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void EncoderMatchesTheReaderModel()
        {
            var memory = new JobMemory();
            using var encoder = new TailRecordEncoder(memory);
            var golden = 0;
            foreach (var file in Directory.EnumerateFiles("../../../testdata", "*.json", SearchOption.AllDirectories).Where(x => x.Contains("_delta_log") && !x.Contains(".checkpoint.")))
            {
                foreach (var line in File.ReadAllLines(file).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    AssertParity(encoder, line);
                    golden++;
                }
            }
            Assert.True(golden > 20);

            var random = new Random(41);
            var rejected = 0;
            for (int i = 0; i < 10_000; i++)
            {
                var line = RandomActions.Line(random);
                AssertParity(encoder, line);
                rejected += ReaderRecords(line) == null ? 1 : 0;
            }
            // Both acceptance and rejection are exercised
            Assert.InRange(rejected, 100, 9_000);
        }

        [Fact]
        public void DuplicateMapKeysFollowTheReader()
        {
            var line = "{\"add\":{\"path\":\"p\",\"partitionValues\":{\"a\":\"1\",\"b\":null,\"a\":\"2\",\"\\u0061\":\"3\"},\"size\":1}}";
            var memory = new JobMemory();
            using var encoder = new TailRecordEncoder(memory);
            AssertParity(encoder, line);

            var add = CheckpointRecord.ToAction(TestRecords.FromAction(JsonSerializer.Deserialize<DeltaAction>(line)!)[0]).Add!;
            Assert.Equal("3", add.PartitionValues!["a"]);
            Assert.Equal(2, add.PartitionValues.Count);
        }

        [Fact]
        public void SupersededMapEntriesAreNotEncoded()
        {
            // The record itself, not its dictionary view, which would collapse duplicates again
            var line = "{\"add\":{\"path\":\"p\",\"partitionValues\":{\"a\":\"1\",\"b\":null,\"a\":\"2\",\"\\u0061\":\"3\"},\"size\":1}}";
            using var encoder = new TailRecordEncoder(new JobMemory());
            int? entries = null;
            encoder.Encode(Encoding.UTF8.GetBytes(line), record =>
            {
                var reader = new RecordReader(record);
                reader.ReadKind(out _);
                reader.SkipString();
                entries = reader.ReadCount();
            });
            Assert.Equal(2, entries);
        }

        [Fact]
        public void LoneSurrogateEscapeDeclinesBeforeAnyRecord()
        {
            // The reader rejects the line; the add before the bad remove must not reach the sink
            using var encoder = new TailRecordEncoder(new JobMemory());
            var records = 0;
            Assert.Throws<CheckpointDeclinedException>(() => encoder.Encode("{\"add\":{\"path\":\"a\"},\"remove\":{\"path\":\"\\udc00\"}}"u8, _ => records++));
            Assert.Equal(0, records);
        }

        [Fact]
        public void CommitTimestampIsReported()
        {
            var memory = new JobMemory();
            using var encoder = new TailRecordEncoder(memory);
            Assert.Equal(0, encoder.Encode("{\"commitInfo\":{\"timestamp\":1760000000123,\"operation\":\"WRITE\"}}"u8, _ => { }));
            Assert.Equal(1760000000123, encoder.CommitTimestamp);
            encoder.Encode("{\"add\":{\"path\":\"a\"}}"u8, _ => { });
            Assert.Null(encoder.CommitTimestamp);
        }

        [Fact]
        public void EncoderReservesBeforeEveryGrowth()
        {
            var lines = new[]
            {
                "{\"add\":{\"path\":\"a\",\"partitionValues\":{\"\\u006b\":\"v\",\"k\":\"w\"}}}",
                "{\"metaData\":{\"id\":\"" + new string('x', 5000) + "\",\"configuration\":{\"\\u0061" + new string('y', 3000) + "\":\"1\",\"a" + new string('y', 3000) + "\":\"2\"}}}",
                "{\"add\":{\"path\":\"" + new string('z', 100_000) + "\",\"tags\":{" + string.Join(",", Enumerable.Range(0, 2000).Select(i => $"\"t{i}\":\"v\"")) + "}}}"
            };
            // Count the reservations of a clean run, then fail each of them in turn
            var clean = new JobMemory();
            var reservations = 0;
            clean.BeforeReserveForTests = _ => reservations++;
            using (var encoder = new TailRecordEncoder(clean))
            {
                foreach (var line in lines)
                {
                    encoder.Encode(Encoding.UTF8.GetBytes(line), _ => { });
                }
            }
            Assert.Equal(0, clean.Managed);
            Assert.True(reservations >= 4);

            for (int failAt = 0; failAt < reservations; failAt++)
            {
                var memory = new JobMemory();
                var seen = 0;
                memory.BeforeReserveForTests = bytes =>
                {
                    if (seen++ == failAt)
                    {
                        throw new OutOfMemoryException("injected");
                    }
                };
                var encoder = new TailRecordEncoder(memory);
                Assert.Throws<OutOfMemoryException>(() =>
                {
                    foreach (var line in lines)
                    {
                        encoder.Encode(Encoding.UTF8.GetBytes(line), _ => { });
                    }
                });
                encoder.Dispose();
                Assert.Equal(0, memory.Managed);
            }
        }

        [Fact]
        public void ManySmallMapEntriesStayWithinTheLineWorkspace()
        {
            // A line near the cap made of tiny entries, right at the entry cap
            var builder = new StringBuilder("{\"add\":{\"path\":\"p\",\"tags\":{");
            for (int i = 0; i < TailRecordEncoder.DefaultEntryCap; i++)
            {
                builder.Append(i == 0 ? "" : ",").Append("\"\\u0061").Append(i).Append("\":\"").Append(new string('v', 40)).Append('"');
            }
            builder.Append("}}}");
            var line = Encoding.UTF8.GetBytes(builder.ToString());
            Assert.InRange(line.Length, 3 << 20, 4 << 20);

            var memory = new JobMemory();
            using var encoder = new TailRecordEncoder(memory);
            var records = 0;
            var allocated = GC.GetAllocatedBytesForCurrentThread();
            encoder.Encode(line, record => records++);
            allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;

            Assert.Equal(1, records);
            // The line workspace J less the line buffer, which the line reader holds
            const long workspace = (16L << 20) - (4L << 20);
            Assert.True(memory.ManagedPeak <= workspace, $"peak {memory.ManagedPeak}");
            Assert.True(allocated <= memory.ManagedPeak + (64 << 10), $"allocated {allocated} with {memory.ManagedPeak} reserved");
        }

        [Fact]
        public void ActionOverTheEntryCapDeclinesBeforeBuffering()
        {
            var line = "{\"add\":{\"path\":\"p\",\"partitionValues\":{" + string.Join(",", Enumerable.Range(0, 11).Select(i => $"\"k{i}\":\"v\"")) + "}}}";
            var memory = new JobMemory();
            using var encoder = new TailRecordEncoder(memory, entryCap: 10);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => encoder.Encode(Encoding.UTF8.GetBytes(line), _ => { }));

            Assert.Equal(MaintenanceReason.EntriesOverCap, declined.Reason);
            Assert.Equal(DeclineDependency.Tail, declined.Dependency);
            Assert.Equal(0, memory.ManagedPeak);
        }

        [Fact]
        public void EntryCapCountsAllCollectionsInAnAction()
        {
            var sixEntries = string.Join(",", Enumerable.Range(0, 6).Select(i => $"\"k{i}\":\"v\""));
            var line = Encoding.UTF8.GetBytes("{\"add\":{\"path\":\"p\",\"partitionValues\":{" + sixEntries + "},\"tags\":{" + sixEntries + "}}}");
            var memory = new JobMemory();
            using var encoder = new TailRecordEncoder(memory, entryCap: 10);
            var error = Assert.Throws<CheckpointDeclinedException>(() => encoder.Encode(line, _ => { }));
            Assert.Equal(MaintenanceReason.EntriesOverCap, error.Reason);
            Assert.Equal(0, memory.ManagedPeak);
        }

        [Theory]
        [InlineData("{}")]
        [InlineData("null")]
        public void DuplicateCommitInfoUsesOnlyTheLastTimestamp(string lastValue)
        {
            var line = "{\"commitInfo\":{\"timestamp\":1},\"commitInfo\":" + lastValue + "}";
            var reader = JsonSerializer.Deserialize<DeltaAction>(line)!;
            Assert.True(reader.CommitInfo?.Data?.ContainsKey("timestamp") != true);

            using var encoder = new TailRecordEncoder(new JobMemory());
            encoder.Encode(Encoding.UTF8.GetBytes(line), _ => { });
            Assert.Null(encoder.CommitTimestamp);
        }

        [Fact]
        public void SupersededInvalidSurrogateStillDeclinesLikeTheReader()
        {
            var line = "{\"add\":{\"path\":\"\\uD800\",\"path\":\"valid\"}}";
            Assert.ThrowsAny<Exception>(() => JsonSerializer.Deserialize<DeltaAction>(line));
            using var encoder = new TailRecordEncoder(new JobMemory());
            var error = Assert.Throws<CheckpointDeclinedException>(() => encoder.Encode(Encoding.UTF8.GetBytes(line), _ => { }));
            Assert.Equal(MaintenanceReason.InvalidUtf8, error.Reason);
        }

        // NAME is a property name long enough that a name lookup does not unescape it
        [Theory]
        [InlineData("{\"NAME\":1,\"add\":{\"path\":\"p\"}}")]
        [InlineData("{\"add\":{\"path\":\"p\",\"NAME\":1}}")]
        [InlineData("{\"add\":{\"path\":\"p\"},\"commitInfo\":{\"NAME\":1}}")]
        [InlineData("{\"add\":{\"path\":\"p\"},\"commitInfo\":{\"flowtide.stageId\":\"\\ud800\"}}")]
        [InlineData("{\"add\":{\"path\":\"a\"},\"remove\":{\"path\":\"r\",\"partitionValues\":{\"\\udc00\":\"v\"}}}")]
        public void EveryTokenTheReaderUnescapesIsCheckedBeforeAnyRecord(string template)
        {
            var line = template.Replace("NAME", "\\ud800" + new string('x', 150));
            Assert.ThrowsAny<Exception>(() => JsonSerializer.Deserialize<DeltaAction>(line));
            using var encoder = new TailRecordEncoder(new JobMemory());
            var emitted = 0;
            var error = Record.Exception(() => encoder.Encode(Encoding.UTF8.GetBytes(line), _ => emitted++));
            Assert.Equal(0, emitted);
            Assert.Equal(MaintenanceReason.InvalidUtf8, Assert.IsType<CheckpointDeclinedException>(error).Reason);
        }

        [Fact]
        public void EntryCapHoldsPerActionNotPerLine()
        {
            // Two actions under the cap whose entries together are over it
            var sixEntries = string.Join(",", Enumerable.Range(0, 6).Select(i => $"\"k{i}\":\"v\""));
            var line = Encoding.UTF8.GetBytes("{\"add\":{\"path\":\"a\",\"partitionValues\":{" + sixEntries + "}},\"remove\":{\"path\":\"r\",\"partitionValues\":{" + sixEntries + "}}}");
            using var encoder = new TailRecordEncoder(new JobMemory(), entryCap: 10);
            var records = 0;
            encoder.Encode(line, _ => records++);
            Assert.Equal(2, records);
        }

        [Fact]
        public void InvalidSurrogateDeclinesBeforeEmittingAnyRecord()
        {
            var line = "{\"add\":{\"path\":\"valid\"},\"txn\":{\"appId\":\"\\uD800\"}}";
            Assert.ThrowsAny<Exception>(() => JsonSerializer.Deserialize<DeltaAction>(line));
            using var encoder = new TailRecordEncoder(new JobMemory());
            var emitted = 0;
            var error = Record.Exception(() => encoder.Encode(Encoding.UTF8.GetBytes(line), _ => emitted++));
            Assert.Equal(0, emitted);
            Assert.IsType<CheckpointDeclinedException>(error);
        }

        [Fact]
        public void EscapedStringAcceptanceMatchesReaderAcrossContexts()
        {
            var values = new[] { "x", "\\u0061", "\\uD83D\\uDE00", "\\uD800", "\\uDC00", "\\uD800x", "\\uD800\\n", "\\uD800\\uD800", "\\uDC00\\uD800", "\\\\uD800", "\\uD800\\\\uDC00", "é\\uD83D\\uDE00" };
            var templates = new[]
            {
                "{\"add\":{\"path\":\"VALUE\"}}",
                "{\"add\":{\"path\":\"VALUE\",\"path\":\"good\"}}",
                "{\"add\":{\"path\":\"a\",\"tags\":{\"VALUE\":\"x\"}}}",
                "{\"add\":{\"path\":\"a\",\"tags\":{\"x\":\"VALUE\"}}}",
                "{\"add\":{\"path\":\"a\"},\"txn\":{\"appId\":\"VALUE\"}}",
                "{\"metaData\":{\"partitionColumns\":[\"VALUE\"]}}",
                "{\"metaData\":{\"format\":{\"VALUE\":false}}}",
                "{\"VALUE\":null,\"add\":{\"path\":\"a\"}}",
                "{\"add\":{\"VALUE\":null,\"path\":\"a\"}}",
                "{\"add\":{\"unknown\":{\"VALUE\":null},\"path\":\"a\"}}",
                "{\"commitInfo\":{\"VALUE\":null}}",
                "{\"commitInfo\":{\"unknown\":\"VALUE\"}}",
                "{\"commitInfo\":{\"flowtide.stageId\":\"VALUE\"}}"
            };
            using var encoder = new TailRecordEncoder(new JobMemory());
            foreach (var template in templates)
            foreach (var value in values)
            {
                var line = template.Replace("VALUE", value);
                DeltaAction? expected = null;
                var referenceError = Record.Exception(() => expected = JsonSerializer.Deserialize<DeltaAction>(line));
                var output = new List<byte[]>();
                var encoderError = Record.Exception(() => encoder.Encode(Encoding.UTF8.GetBytes(line), record => output.Add(record.ToArray())));
                Assert.True((referenceError == null) == (encoderError == null), $"Reader: {referenceError?.GetType().Name}; encoder: {encoderError?.GetType().Name}; line: {line}");
                if (referenceError != null)
                {
                    Assert.IsType<CheckpointDeclinedException>(encoderError);
                    Assert.Empty(output);
                }
                else
                {
                    Assert.Equal(TestRecords.FromAction(expected!).Select(record => TailRecordEncoderTests.RawCanonical(record)), output.Select(record => TailRecordEncoderTests.RawCanonical(record)));
                }
            }
        }
    }
}
