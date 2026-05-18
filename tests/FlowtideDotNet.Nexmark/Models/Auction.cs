// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Models;

/// <summary>
/// Represents an open auction in the NEXMark benchmark.
/// </summary>
public sealed class Auction
{
    public required int Id { get; init; }
    public required int ItemId { get; init; }
    public required int SellerId { get; init; }
    public required int Category { get; init; }
    public required int InitialPrice { get; init; }
    public int? Reserve { get; init; }
    public string? Privacy { get; init; }
    public int? Quantity { get; init; }
    public string? Type { get; init; }
    public long? StartTime { get; init; }
    public long? EndTime { get; init; }
    public required long DateTime { get; init; }
}
