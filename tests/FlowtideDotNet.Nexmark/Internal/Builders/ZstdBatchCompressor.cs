using FlowtideDotNet.Core.ColumnStore.Serialization;
using System;
using ZstdSharp;

namespace FlowtideDotNet.Nexmark.Internal.Builders;

internal class ZstdBatchCompressor : IBatchCompressor, IDisposable
{
    private Compressor _compressor;

    public ZstdBatchCompressor()
    {
        _compressor = new Compressor();
    }

    public void ColumnChange(int columnIndex)
    {
    }

    public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
    {
        return _compressor.Wrap(input, output);
    }

    public void Dispose()
    {
        _compressor.Dispose();
    }
}
