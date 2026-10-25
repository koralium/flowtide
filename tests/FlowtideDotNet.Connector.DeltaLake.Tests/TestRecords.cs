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
    // Encodes reader-model actions as checkpoint records, for tests that write checkpoints without a log
    internal static class TestRecords
    {
        /// <summary>
        /// The records of one log action, in the order the reader applies them; commitInfo and cdc give none.
        /// </summary>
        public static List<byte[]> FromAction(DeltaAction action, bool parsedOnly = false)
        {
            var records = new List<byte[]>();
            if (action.Add != null)
            {
                records.Add(Encode((ref RecordWriter w) => WriteAdd(ref w, action.Add, parsedOnly)));
            }
            if (action.Remove != null)
            {
                records.Add(Encode((ref RecordWriter w) => WriteRemove(ref w, action.Remove)));
            }
            if (action.MetaData != null)
            {
                records.Add(Encode((ref RecordWriter w) => WriteMetaData(ref w, action.MetaData)));
            }
            if (action.Protocol != null)
            {
                records.Add(Encode((ref RecordWriter w) => WriteProtocol(ref w, action.Protocol)));
            }
            if (action.Txn?.AppId != null)
            {
                records.Add(Encode((ref RecordWriter w) => WriteTxn(ref w, action.Txn)));
            }
            if (action.DomainMetadata?.Domain != null)
            {
                records.Add(Encode((ref RecordWriter w) => WriteDomain(ref w, action.DomainMetadata)));
            }
            return records;
        }

        private delegate void WriteFields(ref RecordWriter writer);

        private static byte[] Encode(WriteFields write)
        {
            var counter = RecordWriter.Counting();
            write(ref counter);
            var bytes = new byte[counter.Position];
            var writer = new RecordWriter(bytes);
            write(ref writer);
            return bytes;
        }

        private static void WriteAdd(ref RecordWriter w, DeltaAddAction add, bool parsedOnly)
        {
            w.WriteKind(RecordKind.Add, parsedOnly);
            w.WriteString(add.Path);
            w.WriteMap(add.PartitionValues);
            w.WriteLong(add.Size);
            w.WriteLong(add.ModificationTime);
            w.WriteBool(add.DataChange);
            w.WriteString(add.Statistics);
            w.WriteMap(add.Tags);
            WriteVector(ref w, add.DeletionVector);
            w.WriteLong(add.BaseRowId);
            w.WriteLong(add.DefaultRowCommitVersion);
            w.WriteString(add.ClusteringProvider);
        }

        private static void WriteRemove(ref RecordWriter w, DeltaRemoveFileAction remove)
        {
            w.WriteKind(RecordKind.Remove, false);
            w.WriteString(remove.Path);
            w.WriteLong(remove.DeletionTimestamp);
            w.WriteBool(remove.DataChange);
            w.WriteBool(remove.ExtendedFileMetadata);
            w.WriteMap(remove.PartitionValues);
            w.WriteLong(remove.Size);
            WriteVector(ref w, remove.DeletionVector);
            w.WriteLong(remove.BaseRowId);
            w.WriteLong(remove.DefaultRowCommitVersion);
        }

        private static void WriteVector(ref RecordWriter w, DeletionVector? vector)
        {
            w.WriteStruct(vector != null);
            if (vector != null)
            {
                w.WriteString(vector.StorageType);
                w.WriteString(vector.PathOrInlineDv);
                w.WriteLong(vector.Offset);
                w.WriteLong(vector.SizeInBytes);
                w.WriteLong(vector.Cardinality);
            }
        }

        private static void WriteMetaData(ref RecordWriter w, DeltaMetadataAction metaData)
        {
            w.WriteKind(RecordKind.MetaData, false);
            w.WriteString(metaData.Id);
            w.WriteString(metaData.Name);
            w.WriteString(metaData.Description);
            w.WriteStruct(metaData.Format != null);
            if (metaData.Format != null)
            {
                w.WriteString(metaData.Format.Provider);
                w.WriteMap(metaData.Format.Options);
            }
            w.WriteString(metaData.SchemaString);
            w.WriteList(metaData.PartitionColumns);
            w.WriteLong(metaData.CreatedTime);
            w.WriteMap(metaData.Configuration);
        }

        private static void WriteProtocol(ref RecordWriter w, DeltaProtocolAction protocol)
        {
            w.WriteKind(RecordKind.Protocol, false);
            w.WriteLong(protocol.MinReaderVersion);
            w.WriteLong(protocol.MinWriterVersion);
            w.WriteList(protocol.ReaderFeatures);
            w.WriteList(protocol.WriterFeatures);
        }

        private static void WriteTxn(ref RecordWriter w, DeltaTransactionAction txn)
        {
            w.WriteKind(RecordKind.Txn, false);
            w.WriteString(txn.AppId);
            w.WriteLong(txn.Version);
            w.WriteLong(txn.LastUpdated);
        }

        private static void WriteDomain(ref RecordWriter w, DeltaDomainMetadataAction domain)
        {
            w.WriteKind(RecordKind.Domain, false);
            w.WriteString(domain.Domain);
            w.WriteString(domain.Configuration);
            w.WriteBool(domain.Removed);
        }

        private static void WriteMap(this ref RecordWriter w, IReadOnlyDictionary<string, string>? map)
        {
            if (map == null)
            {
                w.WriteNull();
                return;
            }
            w.WriteCount(map.Count);
            foreach (var entry in map)
            {
                w.WriteString(entry.Key);
                w.WriteString(entry.Value);
            }
        }

        private static void WriteList(this ref RecordWriter w, IReadOnlyList<string>? list)
        {
            if (list == null)
            {
                w.WriteNull();
                return;
            }
            w.WriteCount(list.Count);
            for (int i = 0; i < list.Count; i++)
            {
                w.WriteString(list[i]);
            }
        }

        // A record's reconciliation key as a new array
        public static byte[] Key(ReadOnlySpan<byte> record)
        {
            var key = new byte[RecordKey.Length(record)];
            RecordKey.Write(record, key);
            return key;
        }
    }
}
