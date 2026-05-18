// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;

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
    }

    private IColumn[] CreateColumns()
    {
        var cols = new IColumn[4];
        for (int i = 0; i < 4; i++)
        {
            cols[i] = Column.Create(_memoryAllocator);
        }
        return cols;
    }

    public void Add(Bid bid)
    {
        _currentColumns[0].Add(new Int64Value(bid.AuctionId));
        _currentColumns[1].Add(new Int64Value(bid.BidderId));
        _currentColumns[2].Add(new Int64Value(bid.Price));
        _currentColumns[3].Add(new Int64Value(bid.DateTime));

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
}
