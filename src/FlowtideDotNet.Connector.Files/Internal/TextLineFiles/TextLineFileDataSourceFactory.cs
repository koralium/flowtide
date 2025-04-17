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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Connector.Files.Internal.XmlFiles;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Files.Internal.TextLineFiles
{
    internal class TextLineFileDataSourceFactory : IConnectorSourceFactory, IConnectorTableProviderFactory, ITableProvider
    {
        private readonly string _tableName;
        private readonly TextLineInternalOptions _options;
        private NamedStruct _schema;

        public TextLineFileDataSourceFactory(string tableName, TextLineInternalOptions options)
        {
            this._tableName = tableName;
            this._options = options;
            _schema = new NamedStruct()
            {
                Names = new List<string> { "fileName", "value" },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>() { new StringType(), new StringType() }
                }
            };

            foreach(var extraColumn in options.ExtraColumns)
            {
                _schema.Names.Add(extraColumn.ColumnName);
                _schema.Struct.Types.Add(extraColumn.DataType);
            }
        }

        public bool CanHandle(ReadRelation readRelation)
        {
            return readRelation.NamedTable.DotSeperated.Equals(_tableName, StringComparison.OrdinalIgnoreCase);
        }

        public ITableProvider Create()
        {
            return this;
        }

        public IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new TextLineFileDataSource(_options, readRelation, dataflowBlockOptions);
        }

        public Relation ModifyPlan(ReadRelation readRelation)
        {
            if (readRelation.Filter != null)
            {
                var filterRel = new FilterRelation()
                {
                    Condition = readRelation.Filter,
                    Input = readRelation
                };
                readRelation.Filter = default;
                readRelation.Emit = default;
                return filterRel;
            }
            return readRelation;
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            var fullName = string.Join(".", tableName);
            if (fullName.Equals(_tableName, StringComparison.OrdinalIgnoreCase))
            {
                tableMetadata = new TableMetadata(fullName, _schema);
                return true;
            }
            tableMetadata = default;
            return false;
            throw new NotImplementedException();
        }
    }
}
