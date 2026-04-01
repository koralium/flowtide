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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    /// <summary>
    /// Traverses a query plan to identify and collect input and output tables for lineage analysis.
    /// </summary>
    /// <remarks>This visitor inspects read and write relations within a query plan to extract metadata about
    /// the tables involved. The collected input and output tables can be accessed through the InputTables and
    /// OutputTables properties. This class is intended for internal use within lineage tracking and optimization
    /// workflows.</remarks>
    internal class LineageInputOutputFinderVisitor : OptimizerBaseVisitor
    {
        private readonly IConnectorManager connectorManager;
        private readonly bool includeSchema;
        private Dictionary<string, TableLineageMetadata> _metadatas = new Dictionary<string, TableLineageMetadata>();

        private Dictionary<string, LineageInputTable> _inputTables = new Dictionary<string, LineageInputTable>();
        private Dictionary<string, LineageOutputTable> _outputTables = new Dictionary<string, LineageOutputTable>();

        public LineageInputOutputFinderVisitor(IConnectorManager connectorManager, bool includeSchema)
        {
            this.connectorManager = connectorManager;
            this.includeSchema = includeSchema;
        }

        public Dictionary<string, LineageInputTable> InputTables => _inputTables;

        public Dictionary<string, LineageOutputTable> OutputTables => _outputTables;

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            var key = readRelation.NamedTable.DotSeperated;
            if (!_inputTables.ContainsKey(key))
            {
                var sourceFactory = connectorManager.GetSourceFactory(readRelation);
                var metadata = sourceFactory.GetLineageMetadata(readRelation, includeSchema);

                var inputTable = new LineageInputTable(metadata.Namespace, metadata.TableName);
                if (includeSchema)
                {
                    var schema = metadata.Schema;
                    if (schema == null)
                    {
                        schema = readRelation.BaseSchema;
                    }
                    inputTable.Facets.Schema = LineageSchemaConverter.ConvertToFacet(schema);
                }

                _inputTables.Add(key, inputTable);
                _metadatas.Add(key, metadata);
            }
            
            return base.VisitReadRelation(readRelation, state);
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            var key = writeRelation.NamedObject.DotSeperated;

            if (!_outputTables.ContainsKey(key))
            {
                var sinkFactory = connectorManager.GetSinkFactory(writeRelation);
                var metadata = sinkFactory.GetLineageMetadata(writeRelation, includeSchema);


                var outputTable = new LineageOutputTable(metadata.Namespace, metadata.TableName);
                if (includeSchema)
                {
                    var schema = metadata.Schema;
                    if (schema == null)
                    {
                        schema = writeRelation.TableSchema;
                    }
                    outputTable.Facets.Schema = LineageSchemaConverter.ConvertToFacet(schema);
                }
                
                _metadatas.Add(key, metadata);
                _outputTables.Add(key, outputTable);
            }
            
            return base.VisitWriteRelation(writeRelation, state);
        }
    }
}
