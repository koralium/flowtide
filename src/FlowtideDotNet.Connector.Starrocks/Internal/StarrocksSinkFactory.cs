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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.StarRocks.Internal
{
    internal class StarRocksSinkFactory : AbstractConnectorSinkFactory, ITableProvider
    {
        private readonly StarRocksSinkOptions _options;
        private Dictionary<StarRocksTableKey, TableMetadata>? _tablesCache;

        public StarRocksSinkFactory(StarRocksSinkOptions options)
        {
            this._options = options;
        }

        public override bool CanHandle(WriteRelation writeRelation)
        {
            return true;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            if (!TryGetTableInformation(writeRelation.NamedObject.Names, out var metadata))
            {
                throw new InvalidOperationException($"Table '{string.Join(".", writeRelation.NamedObject.Names)}' not found in Starrocks.");
            }

            // Validate that all insert fields exist in starrocks table
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                var insertName = writeRelation.TableSchema.Names[i];

                bool foundName = false;
                foreach(var name in metadata.Schema.Names)
                {
                    if (insertName.Equals(name, StringComparison.OrdinalIgnoreCase))
                    {
                        foundName = true;
                    }
                }
                if (!foundName)
                {
                    throw new InvalidOperationException($"Column '{insertName}' not found in Starrocks table '{string.Join(".", writeRelation.NamedObject.Names)}'.");
                }
            }

            return new StarRocksPrimaryKeySink(_options, _options.ExecutionMode, writeRelation, dataflowBlockOptions);
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (tableName.Count != 2)
            {
                tableMetadata = default;
                return false;
            }

            if (_tablesCache == null)
            {
                _tablesCache = StarRocksUtils.GetTables(_options).GetAwaiter().GetResult();
            }

            var key = new StarRocksTableKey(tableName[0], tableName[1]);

            return _tablesCache.TryGetValue(key, out tableMetadata);
        }
    }
}
