// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Diagnostics;

namespace FlowtideDotNet.Nexmark.Internal;

/// <summary>
/// Manages the collection of open auctions with virtual/physical ID mapping,
/// dynamic resizing, and chunk-based distribution for random ID selection.
/// Port of OpenAuctions.java.
/// </summary>
internal sealed class AuctionManager
{
    public const int ItemDistrSize = 100;

    // Physical array management
    private int _currPhysId;
    private int _offset;
    private int _numSeqClosed;
    private int _lowChunk;
    private int _highChunk = 1;
    private int _allocSize;
    private AuctionState?[] _openAuctions;
    private readonly JavaRandom _rnd;
    private readonly SimpleCalendar _cal;

    public AuctionManager(SimpleCalendar cal, JavaRandom rnd)
    {
        _allocSize = 3000;
        _openAuctions = new AuctionState?[_allocSize];
        _cal = cal;
        _rnd = rnd;
    }

    /// <summary>
    /// Creates a new open auction and returns its virtual ID.
    /// </summary>
    public int GetNewId()
    {
        CheckSpace();

        Debug.Assert(_currPhysId + _offset != int.MaxValue, "Virtual Id is going to overflow");

        int virtualId = _currPhysId + _offset;
        var newItem = new AuctionState(_cal, virtualId, _rnd);
        _openAuctions[_currPhysId] = newItem;
        _currPhysId++;

        if (virtualId == _highChunk * ItemDistrSize)
        {
            _highChunk++;
        }

        return virtualId;
    }

    /// <summary>
    /// Returns the virtual ID of a random existing open (non-closed) auction.
    /// Also records a bid on that auction.
    /// </summary>
    public int GetExistingId()
    {
        int id;
        do
        {
            id = _rnd.Next(ItemDistrSize);
            id += GetRandomChunkOffset();
        } while (id >= _currPhysId || _openAuctions[id]!.IsClosed(_cal));

        _openAuctions[id]!.RecordBid();
        return id + _offset; // return virtual id
    }

    public int IncreasePrice(int id)
    {
        return _openAuctions[id - _offset]!.IncreasePrice();
    }

    public long GetEndTime(int id)
    {
        return _openAuctions[id - _offset]!.EndTime;
    }

    public int GetCurrPrice(int id)
    {
        return _openAuctions[id - _offset]!.CurrPrice;
    }

    private void CheckSpace()
    {
        if (_currPhysId == _allocSize)
        {
            Shrink();
            if (_currPhysId == _allocSize)
            {
                Expand();
            }
        }
    }

    private void Shrink()
    {
        if (_numSeqClosed >= 500)
        {
            for (int i = _numSeqClosed; i < _allocSize; i++)
            {
                _openAuctions[i - _numSeqClosed] = _openAuctions[i];
            }
            _offset += _numSeqClosed;
            _currPhysId -= _numSeqClosed;
            _numSeqClosed = 0;
            while (_offset >= (_lowChunk + 1) * ItemDistrSize)
            {
                _lowChunk++;
            }
        }
    }

    private void Expand()
    {
        int oldSize = _allocSize;
        _allocSize *= 2;
        var newArray = new AuctionState?[_allocSize];
        for (int i = _numSeqClosed; i < oldSize; i++)
        {
            newArray[i - _numSeqClosed] = _openAuctions[i];
        }
        _offset += _numSeqClosed;
        _currPhysId -= _numSeqClosed;
        _numSeqClosed = 0;
        _openAuctions = newArray;
        while (_offset >= (_lowChunk + 1) * ItemDistrSize)
        {
            _lowChunk++;
        }
    }

    private int GetRandomChunkOffset()
    {
        int chunkId = _rnd.Next(_highChunk - _lowChunk) + _lowChunk;
        return (chunkId * ItemDistrSize) - _offset;
    }
}
