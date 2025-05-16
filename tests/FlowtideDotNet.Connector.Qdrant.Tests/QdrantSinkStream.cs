using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Connector.Qdrant.Extensions;
using FlowtideDotNet.Connector.Qdrant.Internal;
using FlowtideDotNet.Core;

namespace FlowtideDotNet.Connector.Qdrant.Tests
{
    public class QdrantSinkStream : FlowtideTestStream
    {
        private readonly QdrantSinkOptions _options;
        private readonly IEmbeddingGenerator _generator;
        private readonly IStringChunker? _chunker;
        public QdrantSinkStream(string testName, QdrantSinkOptions options, IEmbeddingGenerator generator, IStringChunker? chunker) : base(testName)
        {
            _options = options;
            _generator = generator;
            _chunker = chunker;
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddQdrantSink("*",
                _options,
                _generator,
                _chunker
            );
        }
    }
}