// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Models;

/// <summary>
/// Represents a bid on an open auction in the NEXMark benchmark.
/// </summary>
public sealed class Bid
{
    public required int AuctionId { get; init; }
    public required int BidderId { get; init; }
    public required int Price { get; init; }
    public required long DateTime { get; init; }
}
