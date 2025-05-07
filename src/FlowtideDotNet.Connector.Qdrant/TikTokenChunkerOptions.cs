using Microsoft.ML.Tokenizers;

namespace FlowtideDotNet.Connector.Qdrant
{
    public class TikTokenChunkerOptions
    {

        /// <summary>
        /// If the <see cref="VectorStringColumnName"/> is above this size, split it into chunks of this size.
        /// </summary>
        public required int TokenChunkSize { get; set; }

        /// <summary>
        /// Optional minimum size of the token chunk, will merge chunk with the previous chunk if the size is below this.
        /// Note that this merge will also occur if the chunk is below the <see cref="TokenChunkOverlap"/>.
        /// </summary>
        public int? MinTokenCunkSize { get; set; }

        /// <summary>
        /// When chunking each chunk will overlap by this many tokens.
        /// </summary>
        public required int TokenChunkOverlap { get; set; }

        public required TiktokenTokenizer Tokenizer { get; set; }
    }
}