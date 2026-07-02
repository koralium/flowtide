using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;

namespace FlowtideDotNet.Nexmark.Internal.Builders;

internal sealed class BidBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private IColumn[] _currentColumns;
    private int _currentRowCount;
    private readonly EventBatchSerializer _eventBatchSerializer;
    private readonly FileStream _fileStream;

    public int EventCount { get; private set; }

    public BidBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize, string baseDir)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _currentColumns = CreateColumns();
        _currentRowCount = 0;
        _eventBatchSerializer = new EventBatchSerializer();
        string filePath = Path.Join(baseDir, "bid_batches.bin");
        _fileStream = File.OpenWrite(filePath);
    }

    public void Add(in Bid bid)
    {
        _currentColumns[0].Add(new Int64Value(bid.AuctionId));
        _currentColumns[1].Add(new Int64Value(bid.BidderId));
        _currentColumns[2].Add(new Int64Value(bid.Price));
        _currentColumns[3].Add(new StringValue(bid.Channel));
        _currentColumns[4].Add(new StringValue(bid.Url));
        _currentColumns[5].Add(new TimestampTzValue(bid.DateTime));
        _currentColumns[6].Add(new StringValue(bid.Extra));

        _currentRowCount++;
        EventCount++;

        if (_currentRowCount >= _batchSize)
        {
            Flush();
        }
    }

    public void Flush()
    {
        if (_currentRowCount > 0)
        {
            var b = new EventBatchData(_currentColumns);
            var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
            _eventBatchSerializer.SerializeEventBatch(bufferWriter, b, b.Count);
            
            var span = bufferWriter.WrittenSpan;
            _fileStream.Write(span);

            _currentColumns = CreateColumns();
            _currentRowCount = 0;
        }
    }

    public void Complete()
    {
        Flush();
        _fileStream.Dispose();
    }

    private IColumn[] CreateColumns()
    {
        var columns = new IColumn[7];
        for (int i = 0; i < 7; i++)
        {
            columns[i] = Column.Create(_memoryAllocator);
        }
        return columns;
    }
}
