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

namespace FlowtideDotNet.Connector.Starrocks.Internal
{
    internal class StarrocksSinkFactory : AbstractConnectorSinkFactory, ITableProvider
    {
        private readonly StarrocksSinkOptions _options;
        private Dictionary<StarrocksTableKey, TableMetadata>? _tablesCache;

        public StarrocksSinkFactory(StarrocksSinkOptions options)
        {
            this._options = options;
        }

        public override bool CanHandle(WriteRelation writeRelation)
        {
            return true;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new StarrocksPrimaryKeySink(_options, _options.ExecutionMode, writeRelation, dataflowBlockOptions);
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
                _tablesCache = StarrocksUtils.GetTables(_options).GetAwaiter().GetResult();
            }

            var key = new StarrocksTableKey(tableName[0], tableName[1]);

            return _tablesCache.TryGetValue(key, out tableMetadata);
        }
    }
}
