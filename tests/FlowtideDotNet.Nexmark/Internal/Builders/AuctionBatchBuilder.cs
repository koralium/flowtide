using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;

namespace FlowtideDotNet.Nexmark.Internal.Builders;

internal sealed class AuctionBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private IColumn[] _currentColumns;
    private int _currentRowCount;
    private readonly EventBatchSerializer _eventBatchSerializer;
    private FileStream _fileStream;

    public int EventCount { get; private set; }

    public AuctionBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _currentColumns = CreateColumns();
        _currentRowCount = 0;
        _eventBatchSerializer = new EventBatchSerializer();
        _fileStream = File.OpenWrite("auction_batches.bin");
    }

    public void Add(in Auction auction)
    {
        _currentColumns[0].Add(new Int64Value(auction.Id));
        _currentColumns[1].Add(new StringValue(auction.ItemName));
        _currentColumns[2].Add(new StringValue(auction.Description));
        _currentColumns[3].Add(new Int64Value(auction.InitialBid));
        _currentColumns[4].Add(new Int64Value(auction.Reserve));
        _currentColumns[5].Add(new StringValue(auction.DateTime));
        _currentColumns[6].Add(new StringValue(auction.Expires));
        _currentColumns[7].Add(new Int64Value(auction.Seller));
        _currentColumns[8].Add(new Int64Value(auction.Category));
        _currentColumns[9].Add(new StringValue(auction.Extra));

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
        var columns = new IColumn[10];
        for (int i = 0; i < 10; i++)
        {
            columns[i] = Column.Create(_memoryAllocator);
        }
        return columns;
    }
}
