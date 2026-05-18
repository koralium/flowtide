// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Collections;
using FlowtideDotNet.Nexmark.Internal;
using FlowtideDotNet.Nexmark.Models;

namespace FlowtideDotNet.Nexmark;

/// <summary>
/// A C# port of the NEXMark XMLAuctionStreamGenerator.
/// Produces a deterministic stream of Person, Auction, and Bid events.
/// </summary>
public sealed class NexmarkGenerator : IEnumerable<NexmarkEvent>
{
    private readonly int _numGenCalls;
    private readonly int _seed;

    /// <summary>
    /// Initializes a new instance of the NexmarkGenerator.
    /// </summary>
    /// <param name="genCalls">The number of generation loops to run. Defaults to 1000.</param>
    /// <param name="seed">The random seed to use. Defaults to 103984 (from the original Java code).</param>
    public NexmarkGenerator(int genCalls = 1000, int seed = 103984)
    {
        _numGenCalls = genCalls;
        _seed = seed;
    }

    public IEnumerator<NexmarkEvent> GetEnumerator()
    {
        return GenerateEvents().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    private IEnumerable<NexmarkEvent> GenerateEvents()
    {
        var rnd = new JavaRandom(_seed);
        var cal = new SimpleCalendar(rnd);

        // We use the fixed seeds from the original Java implementation for these components,
        // but mix in the master seed in case the user wants to change it.
        var persons = new PersonIdManager(new JavaRandom(_seed ^ 283494));
        var openAuctions = new AuctionManager(cal, new JavaRandom(_seed ^ 18394));
        var personGen = new PersonGenerator(new JavaRandom(_seed ^ 20934));

        // Startup phase: generate 50 persons
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                cal.IncrementTime();
                int personId = persons.GetNewId();
                var person = personGen.Generate(personId, openAuctions, cal.TimeInSecs);
                yield return NexmarkEvent.ForPerson(person);
            }
        }

        // Startup phase: generate 50 open auctions
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                cal.IncrementTime();
                yield return GenerateOpenAuction(openAuctions, persons, cal, rnd);
            }
        }

        // Main generation loop
        int count = 0;
        while (count < _numGenCalls)
        {
            // Generating a person approximately 10th time will give us ~10 items/person
            if (rnd.Next(10) == 0)
            {
                cal.IncrementTime();
                int personId = persons.GetNewId();
                var person = personGen.Generate(personId, openAuctions, cal.TimeInSecs);
                yield return NexmarkEvent.ForPerson(person);
            }

            // Generate on average 1 item
            int numItems = rnd.Next(3);
            if (numItems > 0)
            {
                cal.IncrementTime();
                for (int i = 0; i < numItems; i++)
                {
                    yield return GenerateOpenAuction(openAuctions, persons, cal, rnd);
                }
            }

            // Generate on average 10 bids
            int numBids = rnd.Next(21);
            if (numBids > 0)
            {
                cal.IncrementTime();
                for (int i = 0; i < numBids; i++)
                {
                    int itemId = openAuctions.GetExistingId();
                    var bid = new Bid
                    {
                        AuctionId = itemId,
                        BidderId = persons.GetExistingId(),
                        Price = openAuctions.IncreasePrice(itemId),
                        DateTime = cal.TimeInSecs
                    };
                    yield return NexmarkEvent.ForBid(bid);
                }
            }

            count++;
        }
    }

    private static NexmarkEvent GenerateOpenAuction(AuctionManager openAuctions, PersonIdManager persons, SimpleCalendar cal, JavaRandom rnd)
    {
        int auctionId = openAuctions.GetNewId();
        
        int? reserve = null;
        if (rnd.NextBoolean())
        {
            reserve = (int)Math.Round(openAuctions.GetCurrPrice(auctionId) * (1.2 + (rnd.NextDouble() + 1)));
        }

        string? privacy = null;
        if (rnd.NextBoolean())
        {
            privacy = rnd.NextBoolean() ? "yes" : "no";
        }

        int categoryId = rnd.Next(303);
        int quantity = 1 + rnd.Next(10);
        
        string type = rnd.NextBoolean() ? "Regular" : "Featured";
        if (quantity > 1 && rnd.NextBoolean())
        {
            type += ", Dutch";
        }

        var auction = new Auction
        {
            Id = auctionId,
            ItemId = auctionId,
            SellerId = persons.GetExistingId(),
            Category = categoryId,
            InitialPrice = openAuctions.GetCurrPrice(auctionId),
            Reserve = reserve,
            Privacy = privacy,
            Quantity = quantity,
            Type = type,
            StartTime = cal.TimeInSecs,
            EndTime = openAuctions.GetEndTime(auctionId),
            DateTime = cal.TimeInSecs
        };

        return NexmarkEvent.ForAuction(auction);
    }
}
