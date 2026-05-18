// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Internal;

/// <summary>
/// Tracks per-auction state: current price, end time, closed status.
/// Port of OpenAuction.java.
/// </summary>
internal sealed class AuctionState
{
    public const int MaxAuctionLenSec = 24 * 60 * 60; // 24 hours
    public const int MinAuctionLenSec = 2 * 60 * 60;  // 2 hours

    private int _currPrice;
    private bool _closed;
    private readonly long _endTime;
    private int _numBids;
    private readonly JavaRandom _rnd;

    public AuctionState(SimpleCalendar cal, int virtualId, JavaRandom rnd)
    {
        _currPrice = rnd.Next(200) + 1; // initial price must be at least $1
        _endTime = cal.TimeInSecs + rnd.Next(MaxAuctionLenSec) + MinAuctionLenSec;
        _rnd = rnd;
    }

    /// <summary>
    /// Increases the price by a random amount (1-25) and returns the new price.
    /// </summary>
    public int IncreasePrice()
    {
        int increase = _rnd.Next(25) + 1; // zero increases not allowed
        _currPrice += increase;
        return _currPrice;
    }

    public int CurrPrice => _currPrice;

    public long EndTime => _endTime;

    public bool IsClosed(SimpleCalendar cal)
    {
        CheckClosed(cal);
        return _closed;
    }

    public void RecordBid()
    {
        _numBids++;
    }

    private void CheckClosed(SimpleCalendar cal)
    {
        if (!_closed && cal.TimeInSecs > _endTime)
        {
            _closed = true;
        }
    }
}
