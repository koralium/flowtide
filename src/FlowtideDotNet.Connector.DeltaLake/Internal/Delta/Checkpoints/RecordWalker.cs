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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// The structs of the schema: the six top-level columns, then the nested ones.
    /// </summary>
    internal enum StructNode : byte
    {
        Txn, Add, Remove, MetaData, Protocol, Domain, AddDv, RemoveDv, MetaFormat
    }

    /// <summary>
    /// Receives one row's leaf values in schema order, collections announced by their entry count.
    /// </summary>
    internal interface IRecordVisitor
    {
        void Struct(StructNode node, bool present);

        void String(Leaf leaf, bool present, ReadOnlySpan<byte> utf8);

        void Int64(Leaf leaf, long? value);

        void Bool(Leaf leaf, bool? value);

        void Repeated(RepeatedNode node, int? count);
    }

    /// <summary>
    /// Walks a record, or a null column, as leaf events in schema order.
    /// </summary>
    internal static class RecordWalker
    {
        public static StructNode Column(RecordKind kind) => kind switch
        {
            RecordKind.Txn => StructNode.Txn,
            RecordKind.Add => StructNode.Add,
            RecordKind.Remove => StructNode.Remove,
            RecordKind.MetaData => StructNode.MetaData,
            RecordKind.Protocol => StructNode.Protocol,
            _ => StructNode.Domain
        };

        public static void Walk<TVisitor>(ReadOnlySpan<byte> record, ref TVisitor visitor) where TVisitor : struct, IRecordVisitor
        {
            var r = new RecordReader(record);
            var kind = r.ReadKind(out _);
            visitor.Struct(Column(kind), true);
            switch (kind)
            {
                case RecordKind.Txn:
                    String(ref r, ref visitor, Leaf.TxnAppId);
                    visitor.Int64(Leaf.TxnVersion, r.ReadLong());
                    visitor.Int64(Leaf.TxnLastUpdated, r.ReadLong());
                    break;
                case RecordKind.Add:
                    String(ref r, ref visitor, Leaf.AddPath);
                    Map(ref r, ref visitor, RepeatedNode.AddPartitionValues, Leaf.AddPartitionKey, Leaf.AddPartitionValue);
                    visitor.Int64(Leaf.AddSize, r.ReadLong());
                    visitor.Int64(Leaf.AddModificationTime, r.ReadLong());
                    visitor.Bool(Leaf.AddDataChange, r.ReadBool());
                    String(ref r, ref visitor, Leaf.AddStats);
                    Map(ref r, ref visitor, RepeatedNode.AddTags, Leaf.AddTagKey, Leaf.AddTagValue);
                    Vector(ref r, ref visitor, StructNode.AddDv, Leaf.AddDvStorageType);
                    visitor.Int64(Leaf.AddBaseRowId, r.ReadLong());
                    visitor.Int64(Leaf.AddDefaultRowCommitVersion, r.ReadLong());
                    String(ref r, ref visitor, Leaf.AddClusteringProvider);
                    break;
                case RecordKind.Remove:
                    String(ref r, ref visitor, Leaf.RemovePath);
                    visitor.Int64(Leaf.RemoveDeletionTimestamp, r.ReadLong());
                    visitor.Bool(Leaf.RemoveDataChange, r.ReadBool());
                    visitor.Bool(Leaf.RemoveExtendedFileMetadata, r.ReadBool());
                    Map(ref r, ref visitor, RepeatedNode.RemovePartitionValues, Leaf.RemovePartitionKey, Leaf.RemovePartitionValue);
                    visitor.Int64(Leaf.RemoveSize, r.ReadLong());
                    Vector(ref r, ref visitor, StructNode.RemoveDv, Leaf.RemoveDvStorageType);
                    visitor.Int64(Leaf.RemoveBaseRowId, r.ReadLong());
                    visitor.Int64(Leaf.RemoveDefaultRowCommitVersion, r.ReadLong());
                    break;
                case RecordKind.MetaData:
                    String(ref r, ref visitor, Leaf.MetaId);
                    String(ref r, ref visitor, Leaf.MetaName);
                    String(ref r, ref visitor, Leaf.MetaDescription);
                    var format = r.ReadStruct();
                    visitor.Struct(StructNode.MetaFormat, format);
                    if (format)
                    {
                        String(ref r, ref visitor, Leaf.MetaFormatProvider);
                        Map(ref r, ref visitor, RepeatedNode.MetaFormatOptions, Leaf.MetaFormatOptionKey, Leaf.MetaFormatOptionValue);
                    }
                    else
                    {
                        visitor.String(Leaf.MetaFormatProvider, false, default);
                        visitor.Repeated(RepeatedNode.MetaFormatOptions, null);
                    }
                    String(ref r, ref visitor, Leaf.MetaSchemaString);
                    List(ref r, ref visitor, RepeatedNode.MetaPartitionColumns, Leaf.MetaPartitionColumn);
                    visitor.Int64(Leaf.MetaCreatedTime, r.ReadLong());
                    Map(ref r, ref visitor, RepeatedNode.MetaConfiguration, Leaf.MetaConfigurationKey, Leaf.MetaConfigurationValue);
                    break;
                case RecordKind.Protocol:
                    visitor.Int64(Leaf.ProtocolMinReader, r.ReadLong());
                    visitor.Int64(Leaf.ProtocolMinWriter, r.ReadLong());
                    List(ref r, ref visitor, RepeatedNode.ProtocolReaderFeatures, Leaf.ProtocolReaderFeature);
                    List(ref r, ref visitor, RepeatedNode.ProtocolWriterFeatures, Leaf.ProtocolWriterFeature);
                    break;
                case RecordKind.Domain:
                    String(ref r, ref visitor, Leaf.DomainName);
                    String(ref r, ref visitor, Leaf.DomainConfiguration);
                    visitor.Bool(Leaf.DomainRemoved, r.ReadBool());
                    break;
            }
            if (!r.End)
            {
                throw new InvalidDataException("A record has bytes after its last field");
            }
        }

        /// <summary>
        /// The events of a row whose action is in another column: every leaf of this column null.
        /// </summary>
        public static void WalkNull<TVisitor>(StructNode column, ref TVisitor visitor) where TVisitor : struct, IRecordVisitor
        {
            visitor.Struct(column, false);
            switch (column)
            {
                case StructNode.Txn:
                    visitor.String(Leaf.TxnAppId, false, default);
                    visitor.Int64(Leaf.TxnVersion, null);
                    visitor.Int64(Leaf.TxnLastUpdated, null);
                    break;
                case StructNode.Add:
                    visitor.String(Leaf.AddPath, false, default);
                    visitor.Repeated(RepeatedNode.AddPartitionValues, null);
                    visitor.Int64(Leaf.AddSize, null);
                    visitor.Int64(Leaf.AddModificationTime, null);
                    visitor.Bool(Leaf.AddDataChange, null);
                    visitor.String(Leaf.AddStats, false, default);
                    visitor.Repeated(RepeatedNode.AddTags, null);
                    NullVector(ref visitor, StructNode.AddDv, Leaf.AddDvStorageType);
                    visitor.Int64(Leaf.AddBaseRowId, null);
                    visitor.Int64(Leaf.AddDefaultRowCommitVersion, null);
                    visitor.String(Leaf.AddClusteringProvider, false, default);
                    break;
                case StructNode.Remove:
                    visitor.String(Leaf.RemovePath, false, default);
                    visitor.Int64(Leaf.RemoveDeletionTimestamp, null);
                    visitor.Bool(Leaf.RemoveDataChange, null);
                    visitor.Bool(Leaf.RemoveExtendedFileMetadata, null);
                    visitor.Repeated(RepeatedNode.RemovePartitionValues, null);
                    visitor.Int64(Leaf.RemoveSize, null);
                    NullVector(ref visitor, StructNode.RemoveDv, Leaf.RemoveDvStorageType);
                    visitor.Int64(Leaf.RemoveBaseRowId, null);
                    visitor.Int64(Leaf.RemoveDefaultRowCommitVersion, null);
                    break;
                case StructNode.MetaData:
                    visitor.String(Leaf.MetaId, false, default);
                    visitor.String(Leaf.MetaName, false, default);
                    visitor.String(Leaf.MetaDescription, false, default);
                    visitor.Struct(StructNode.MetaFormat, false);
                    visitor.String(Leaf.MetaFormatProvider, false, default);
                    visitor.Repeated(RepeatedNode.MetaFormatOptions, null);
                    visitor.String(Leaf.MetaSchemaString, false, default);
                    visitor.Repeated(RepeatedNode.MetaPartitionColumns, null);
                    visitor.Int64(Leaf.MetaCreatedTime, null);
                    visitor.Repeated(RepeatedNode.MetaConfiguration, null);
                    break;
                case StructNode.Protocol:
                    visitor.Int64(Leaf.ProtocolMinReader, null);
                    visitor.Int64(Leaf.ProtocolMinWriter, null);
                    visitor.Repeated(RepeatedNode.ProtocolReaderFeatures, null);
                    visitor.Repeated(RepeatedNode.ProtocolWriterFeatures, null);
                    break;
                case StructNode.Domain:
                    visitor.String(Leaf.DomainName, false, default);
                    visitor.String(Leaf.DomainConfiguration, false, default);
                    visitor.Bool(Leaf.DomainRemoved, null);
                    break;
            }
        }

        private static void String<TVisitor>(ref RecordReader r, ref TVisitor visitor, Leaf leaf) where TVisitor : struct, IRecordVisitor
        {
            var present = r.ReadUtf8(out var value);
            visitor.String(leaf, present, value);
        }

        private static void Map<TVisitor>(ref RecordReader r, ref TVisitor visitor, RepeatedNode node, Leaf key, Leaf value) where TVisitor : struct, IRecordVisitor
        {
            var count = r.ReadCount();
            visitor.Repeated(node, count);
            for (int i = 0; i < (count ?? 0); i++)
            {
                if (!r.ReadUtf8(out var keyBytes))
                {
                    throw new InvalidDataException("A map key is null");
                }
                visitor.String(key, true, keyBytes);
                String(ref r, ref visitor, value);
            }
        }

        private static void List<TVisitor>(ref RecordReader r, ref TVisitor visitor, RepeatedNode node, Leaf item) where TVisitor : struct, IRecordVisitor
        {
            var count = r.ReadCount();
            visitor.Repeated(node, count);
            for (int i = 0; i < (count ?? 0); i++)
            {
                String(ref r, ref visitor, item);
            }
        }

        // The five vector leaves follow the storage type leaf in order
        private static void Vector<TVisitor>(ref RecordReader r, ref TVisitor visitor, StructNode node, Leaf first) where TVisitor : struct, IRecordVisitor
        {
            if (!r.ReadStruct())
            {
                NullVector(ref visitor, node, first);
                return;
            }
            visitor.Struct(node, true);
            String(ref r, ref visitor, first);
            String(ref r, ref visitor, first + 1);
            visitor.Int64(first + 2, r.ReadLong());
            visitor.Int64(first + 3, r.ReadLong());
            visitor.Int64(first + 4, r.ReadLong());
        }

        private static void NullVector<TVisitor>(ref TVisitor visitor, StructNode node, Leaf first) where TVisitor : struct, IRecordVisitor
        {
            visitor.Struct(node, false);
            visitor.String(first, false, default);
            visitor.String(first + 1, false, default);
            visitor.Int64(first + 2, null);
            visitor.Int64(first + 3, null);
            visitor.Int64(first + 4, null);
        }
    }
}
