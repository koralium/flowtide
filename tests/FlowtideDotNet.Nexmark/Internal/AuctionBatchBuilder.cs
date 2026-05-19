using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;

namespace FlowtideDotNet.Nexmark.Internal;

internal sealed class AuctionBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private readonly List<EventBatchData> _batches;
    private IColumn[] _currentColumns;
    private int _currentRowCount;

    public AuctionBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize, List<EventBatchData> batches)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _batches = batches;
        _currentColumns = CreateColumns();
        _currentRowCount = 0;
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

        if (_currentRowCount >= _batchSize)
        {
            Flush();
        }
    }

    public void Flush()
    {
        if (_currentRowCount > 0)
        {
            _batches.Add(new EventBatchData(_currentColumns));
            _currentColumns = CreateColumns();
            _currentRowCount = 0;
        }
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
