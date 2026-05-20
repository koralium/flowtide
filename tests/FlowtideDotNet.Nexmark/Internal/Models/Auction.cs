using FlowtideDotNet.Nexmark.Internal.Config;
using System;

namespace FlowtideDotNet.Nexmark.Models;

public struct Auction
{
    public long Id { get; set; }
    public string ItemName { get; set; }
    public string Description { get; set; }
    public long InitialBid { get; set; }
    public long Reserve { get; set; }
    public string DateTime { get; set; }
    public string Expires { get; set; }
    public long Seller { get; set; }
    public long Category { get; set; }
    public string Extra { get; set; }

    public static Auction Generate(long eventsSoFar, long eventId, long time, NexmarkConfig nex)
    {
        var rng = new Random((int)(eventId & 0xFFFFFFFF));
        long id = LastId(eventId, nex) + nex.FirstAuctionId;
        string itemName = rng.GenString(20);
        string description = rng.GenString(100);
        long initialBid = rng.GenPrice();
        long reserve = initialBid + rng.GenPrice();
        string dateTime = NexmarkUtils.MilliTsToTimestampString(time);
        string expires = NexmarkUtils.MilliTsToTimestampString(time + NextLength(eventsSoFar, rng, time, nex));
        
        long seller;
        if (rng.Next(0, nex.HotSellerRatio) > 0)
        {
            seller = (Person.LastId(eventId, nex) / nex.HotSellerRatio2) * nex.HotSellerRatio2;
        }
        else
        {
            seller = Person.NextId(eventId, rng, nex);
        }
        seller += nex.FirstPersonId;
        
        long category = nex.FirstCategoryId + rng.Next(0, nex.NumCategories);

        int currentSize = 8 + itemName.Length + description.Length + 8 + 8 + 8 + 8 + 8;
        string extra = rng.GenNextExtra(currentSize, nex.AvgAuctionByteSize);

        return new Auction
        {
            Id = id,
            ItemName = itemName,
            Description = description,
            InitialBid = initialBid,
            Reserve = reserve,
            DateTime = dateTime,
            Expires = expires,
            Seller = seller,
            Category = category,
            Extra = extra
        };
    }

    public static long NextId(long eventId, Random rng, NexmarkConfig nex)
    {
        long maxAuction = LastId(eventId, nex);
        long minAuction = maxAuction < nex.InFlightAuctions ? 0 : maxAuction - nex.InFlightAuctions;
        return minAuction + rng.Next(0, (int)(maxAuction - minAuction + 1 + nex.AuctionIdLead));
    }

    public static long LastId(long eventId, NexmarkConfig nex)
    {
        long epoch = eventId / nex.ProportionDenominator;
        long offset = eventId % nex.ProportionDenominator;
        if (offset < nex.PersonProportion)
        {
            epoch -= 1;
            offset = nex.AuctionProportion - 1;
        }
        else if (nex.PersonProportion + nex.AuctionProportion <= offset)
        {
            offset = nex.AuctionProportion - 1;
        }
        else
        {
            offset -= nex.PersonProportion;
        }
        return epoch * nex.AuctionProportion + offset;
    }

    private static long NextLength(long eventsSoFar, Random rng, long time, NexmarkConfig nex)
    {
        long currentEvent = nex.NextAdjustedEvent(eventsSoFar);
        long eventsForAuctions = (nex.InFlightAuctions * nex.ProportionDenominator) / nex.AuctionProportion;
        long futureAuction = nex.EventTimestamp(currentEvent + eventsForAuctions);
        long horizon = futureAuction - time;
        return 1 + rng.Next(0, (int)Math.Max(horizon * 2, 1));
    }
}
