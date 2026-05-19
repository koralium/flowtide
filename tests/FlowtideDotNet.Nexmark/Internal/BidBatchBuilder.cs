using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;

namespace FlowtideDotNet.Nexmark.Internal;

internal sealed class BidBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private readonly List<EventBatchData> _batches;
    private IColumn[] _currentColumns;
    private int _currentRowCount;

    public BidBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize, List<EventBatchData> batches)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _batches = batches;
        _currentColumns = CreateColumns();
        _currentRowCount = 0;
    }

    public void Add(in Bid bid)
    {
        _currentColumns[0].Add(new Int64Value(bid.AuctionId));
        _currentColumns[1].Add(new Int64Value(bid.BidderId));
        _currentColumns[2].Add(new Int64Value(bid.Price));
        _currentColumns[3].Add(new StringValue(bid.Channel));
        _currentColumns[4].Add(new StringValue(bid.Url));
        _currentColumns[5].Add(new StringValue(bid.DateTime));
        _currentColumns[6].Add(new StringValue(bid.Extra));

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
        var columns = new IColumn[7];
        for (int i = 0; i < 7; i++)
        {
            columns[i] = Column.Create(_memoryAllocator);
        }
        return columns;
    }
}
