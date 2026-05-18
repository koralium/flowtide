// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Nexmark.Models;

namespace FlowtideDotNet.Nexmark;

/// <summary>
/// The type of a NEXMark event.
/// </summary>
public enum NexmarkEventType
{
    Person,
    Auction,
    Bid
}

/// <summary>
/// Represents a single event in the NEXMark benchmark stream.
/// Each event is exactly one of: Person, Auction, or Bid.
/// </summary>
public sealed class NexmarkEvent
{
    public NexmarkEventType Type { get; }
    public Person? Person { get; }
    public Auction? Auction { get; }
    public Bid? Bid { get; }

    private NexmarkEvent(NexmarkEventType type, Person? person, Auction? auction, Bid? bid)
    {
        Type = type;
        Person = person;
        Auction = auction;
        Bid = bid;
    }

    public static NexmarkEvent ForPerson(Person person) =>
        new(NexmarkEventType.Person, person, null, null);

    public static NexmarkEvent ForAuction(Auction auction) =>
        new(NexmarkEventType.Auction, null, auction, null);

    public static NexmarkEvent ForBid(Bid bid) =>
        new(NexmarkEventType.Bid, null, null, bid);
}
