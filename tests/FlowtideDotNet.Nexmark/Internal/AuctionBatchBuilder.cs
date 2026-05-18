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
    }

    private IColumn[] CreateColumns()
    {
        var cols = new IColumn[12];
        for (int i = 0; i < 12; i++)
        {
            cols[i] = Column.Create(_memoryAllocator);
        }
        return cols;
    }

    public void Add(Auction auction)
    {
        _currentColumns[0].Add(new Int64Value(auction.Id));
        _currentColumns[1].Add(new Int64Value(auction.ItemId));
        _currentColumns[2].Add(new Int64Value(auction.SellerId));
        _currentColumns[3].Add(new Int64Value(auction.Category));
        _currentColumns[4].Add(new Int64Value(auction.InitialPrice));
        
        if (auction.Reserve.HasValue)
            _currentColumns[5].Add(new Int64Value(auction.Reserve.Value));
        else
            _currentColumns[5].Add(NullValue.Instance);

        if (auction.Privacy != null)
            _currentColumns[6].Add(new StringValue(auction.Privacy));
        else
            _currentColumns[6].Add(NullValue.Instance);

        if (auction.Quantity.HasValue)
            _currentColumns[7].Add(new Int64Value(auction.Quantity.Value));
        else
            _currentColumns[7].Add(NullValue.Instance);

        if (auction.Type != null)
            _currentColumns[8].Add(new StringValue(auction.Type));
        else
            _currentColumns[8].Add(NullValue.Instance);

        if (auction.StartTime.HasValue)
            _currentColumns[9].Add(new Int64Value(auction.StartTime.Value));
        else
            _currentColumns[9].Add(NullValue.Instance);

        if (auction.EndTime.HasValue)
            _currentColumns[10].Add(new Int64Value(auction.EndTime.Value));
        else
            _currentColumns[10].Add(NullValue.Instance);

        _currentColumns[11].Add(new Int64Value(auction.DateTime));

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
