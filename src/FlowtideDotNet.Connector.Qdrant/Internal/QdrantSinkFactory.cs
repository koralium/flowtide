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
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    internal class QdrantSinkFactory : RegexConnectorSinkFactory
    {
        private readonly QdrantSinkOptions _options;
        private readonly IEmbeddingGenerator _embeddingsGenerator;
        private readonly IStringChunker? _chunker;

        public QdrantSinkFactory(string regexPattern, QdrantSinkOptions options, IEmbeddingGenerator embeddingsGenerator, IStringChunker? chunker) : base(regexPattern)
        {
            _options = options;
            _embeddingsGenerator = embeddingsGenerator;
            _chunker = chunker;
        }

        public override bool CanHandle(WriteRelation writeRelation)
        {
            if (!base.CanHandle(writeRelation))
            {
                return false;
            }

            var pk = writeRelation.TableSchema.Names.FindIndex(x => x.Equals(_options.IdColumnName, StringComparison.OrdinalIgnoreCase));

            if (pk == -1)
            {
                throw new NotSupportedException($"Qdrant sink requires a primary key column named '{_options.IdColumnName}'. This can be configured by setting the {nameof(_options.IdColumnName)} property.");
            }

            var vectorString = writeRelation.TableSchema.Names.FindIndex(x => x.Equals(_options.VectorStringColumnName, StringComparison.OrdinalIgnoreCase));

            if (vectorString == -1)
            {
                throw new NotSupportedException($"Qdrant sink requires a column named '{_options.VectorStringColumnName}'. This can be configured by setting the {nameof(_options.VectorStringColumnName)} property.");
            }

            return true;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new QdrantSink(_options, writeRelation, dataflowBlockOptions, _embeddingsGenerator, _chunker);
        }
    }
}
