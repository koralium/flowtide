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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal class DeltaTable
    {
        private DeltaMetadataAction _metadata;
        private DeltaProtocolAction _protocol;
        private List<DeltaAddAction> _addFiles;
        private long _version;
        private StructType _schema;

        internal DeltaTable(DeltaMetadataAction metadata, DeltaProtocolAction protocol, List<DeltaAddAction> addFiles, long version)
        {
            _metadata = metadata;
            _protocol = protocol;
            _addFiles = addFiles;
            _version = version;

            // Read schema
            var jsonOptions = new JsonSerializerOptions();
            jsonOptions.Converters.Add(new TypeConverter());
            var schema = JsonSerializer.Deserialize<SchemaBaseType>(_metadata.SchemaString!, jsonOptions);

            if (schema!.Type != SchemaType.Struct)
            {
                throw new Exception("Schema type must be struct");
            }

            _schema = (StructType)schema;
        }

        public List<DeltaAddAction> AddFiles => _addFiles;

        public StructType Schema => _schema;

        public IReadOnlyList<string> PartitionColumns => _metadata.PartitionColumns ?? (IReadOnlyList<string>)ImmutableList<string>.Empty;

        public DeltaMetadataFormat Format => _metadata.Format ?? throw new Exception("Format must be defined");

        public long Version => _version;

        public bool DeleteVectorEnabled
        {
            get
            {
                return _protocol.WriterFeatures?.Contains("deletionVectors") ?? false;
            }
        }
    }
}
