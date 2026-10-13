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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CheckpointRecordTests
    {
        [Fact]
        public void RecordRoundTrips()
        {
            var random = new Random(17);
            for (int i = 0; i < 5000; i++)
            {
                var action = RandomActions.Next(random);
                var records = TestRecords.FromAction(action);
                Assert.Single(records);
                Assert.Equal(RandomActions.Canonical(action), RandomActions.Canonical(CheckpointRecord.ToAction(records[0])));
            }
        }

        [Fact]
        public void ParsedOnlyFlagRoundTrips()
        {
            var add = new DeltaAction() { Add = new DeltaAddAction() { Path = "a" } };
            Assert.True(CheckpointRecord.ParsedOnly(TestRecords.FromAction(add, parsedOnly: true)[0]));
            Assert.False(CheckpointRecord.ParsedOnly(TestRecords.FromAction(add)[0]));
            Assert.Equal(RecordKind.Add, CheckpointRecord.Kind(TestRecords.FromAction(add, parsedOnly: true)[0]));
        }

        [Fact]
        public void AddAndRemoveShareOneKey()
        {
            var vector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "abcdefghijklmnopqrst", Offset = 4, SizeInBytes = 10, Cardinality = 2 };
            var add = TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "p", DeletionVector = vector, Size = 5 } })[0];
            var remove = TestRecords.FromAction(new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "p", DeletionVector = vector, DeletionTimestamp = 7 } })[0];
            var plain = TestRecords.FromAction(new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "p" } })[0];

            Assert.Equal(TestRecords.Key(add), TestRecords.Key(remove));
            Assert.NotEqual(TestRecords.Key(add), TestRecords.Key(plain));
        }

        [Fact]
        public void KeysWithEmbeddedNulDoNotCollide()
        {
            byte[] Key(string path, string? dv) => TestRecords.Key(TestRecords.FromAction(new DeltaAction()
            {
                Add = new DeltaAddAction() { Path = path, DeletionVector = dv == null ? null : new DeletionVector() { StorageType = "", PathOrInlineDv = dv } }
            })[0]);
            byte[] Txn(string id) => TestRecords.Key(TestRecords.FromAction(new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = id } })[0]);
            byte[] Domain(string id) => TestRecords.Key(TestRecords.FromAction(new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = id } })[0]);

            var keys = new List<byte[]>()
            {
                Key("a\0b", null), Key("a", "\0b"), Key("a\0", "b"), Key("a", "b"), Key("ab", null), Key("a", null), Key("a", ""),
                Txn("a\0b"), Txn("a"), Domain("a\0b"), Domain("a")
            };
            Assert.Equal(keys.Count, keys.Select(Convert.ToHexString).Distinct().Count());
        }

        [Fact]
        public void RecordKeysMatchReaderKeys()
        {
            var random = new Random(23);
            var byReader = new Dictionary<DeltaFileKey, string>();
            var byRecord = new Dictionary<string, DeltaFileKey>();
            for (int i = 0; i < 10_000; i++)
            {
                // A small alphabet so many actions share a key
                var vector = random.Next(3) == 0 ? null : new DeletionVector()
                {
                    StorageType = random.Next(2) == 0 ? "u" : "p",
                    PathOrInlineDv = "dv" + random.Next(4),
                    Offset = random.Next(3) == 0 ? null : random.Next(-2, 3),
                    SizeInBytes = random.Next(100),
                    Cardinality = random.Next(100)
                };
                var path = "p" + random.Next(20) + (random.Next(4) == 0 ? "@1" : "");
                DeltaAction action;
                DeltaFileKey readerKey;
                if (random.Next(2) == 0)
                {
                    var add = new DeltaAddAction() { Path = path, DeletionVector = vector };
                    action = new DeltaAction() { Add = add };
                    readerKey = add.GetKey();
                }
                else
                {
                    var remove = new DeltaRemoveFileAction() { Path = path, DeletionVector = vector };
                    action = new DeltaAction() { Remove = remove };
                    readerKey = remove.GetKey();
                }
                var recordKey = Convert.ToHexString(TestRecords.Key(TestRecords.FromAction(action)[0]));
                Assert.Equal(byReader.GetValueOrDefault(readerKey, recordKey), recordKey);
                Assert.Equal(byRecord.GetValueOrDefault(recordKey, readerKey), readerKey);
                byReader[readerKey] = recordKey;
                byRecord[recordKey] = readerKey;
            }
            Assert.True(byReader.Count > 100);
        }

        [Fact]
        public void ProtocolAndMetaDataKeysSortFirst()
        {
            var keys = new[]
            {
                TestRecords.Key(TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "" } })[0]),
                TestRecords.Key(TestRecords.FromAction(new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = "" } })[0]),
                TestRecords.Key(TestRecords.FromAction(new DeltaAction() { MetaData = new DeltaMetadataAction() })[0]),
                TestRecords.Key(TestRecords.FromAction(new DeltaAction() { Protocol = new DeltaProtocolAction() })[0])
            };
            var sorted = keys.OrderBy(x => x, Comparer<byte[]>.Create((x, y) => x.AsSpan().SequenceCompareTo(y))).ToList();
            Assert.Equal(new[] { (byte)RecordFamily.Protocol }, sorted[0]);
            Assert.Equal(new[] { (byte)RecordFamily.MetaData }, sorted[1]);
        }

        [Fact]
        public void NullAndEmptyPathsHaveDifferentKeys()
        {
            // The reader keeps a null path apart from an empty one
            var none = TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = null! } })[0];
            var empty = TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "" } })[0];
            Assert.NotEqual(TestRecords.Key(none), TestRecords.Key(empty));
        }

        [Fact]
        public void CorruptListCountIsRefusedBeforeListAllocation()
        {
            var data = new byte[32];
            var writer = new RecordWriter(data);
            writer.WriteCount(int.MaxValue);
            Array.Resize(ref data, writer.Position);
            var error = Record.Exception(() =>
            {
                var reader = new RecordReader(data);
                reader.ReadList();
            });
            Assert.IsType<InvalidDataException>(error);
        }
    }
}
