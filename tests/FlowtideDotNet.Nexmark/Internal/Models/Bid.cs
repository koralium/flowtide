using FlowtideDotNet.Nexmark.Internal.Config;
using System;

namespace FlowtideDotNet.Nexmark.Models;

public struct Bid
{
    public long AuctionId { get; set; }
    public long BidderId { get; set; }
    public long Price { get; set; }
    public string Channel { get; set; }
    public string Url { get; set; }
    public string DateTime { get; set; }
    public string Extra { get; set; }

    public static Bid Generate(long eventId, long time, NexmarkConfig nex)
    {
        var rng = new Random((int)(eventId & 0xFFFFFFFF));
        
        long auction;
        if (rng.Next(0, nex.HotAuctionRatio) > 0)
        {
            auction = (Auction.LastId(eventId, nex) / nex.HotAuctionRatio2) * nex.HotAuctionRatio2;
        }
        else
        {
            auction = Auction.NextId(eventId, rng, nex);
        }

        long bidder;
        if (rng.Next(0, nex.HotBidderRatio) > 0)
        {
            bidder = (Person.LastId(eventId, nex) / nex.HotBidderRatio2) * nex.HotBidderRatio2 + 1;
        }
        else
        {
            bidder = Person.NextId(eventId, rng, nex);
        }

        long price = rng.GenPrice();

        string channel, url;
        if (rng.Next(0, nex.HotChannelRatio) > 0)
        {
            int index = rng.Next(0, nex.HotChannels.Length);
            channel = nex.HotChannels[index];
            url = nex.HotUrls[index];
        }
        else
        {
            var kvp = nex.ChannelUrlMap[rng.Next(0, NexmarkConfig.ChannelNumber)];
            channel = kvp.Channel;
            url = kvp.Url;
        }

        int currentSize = 8 + 8 + 8 + 8;
        string extra = rng.GenNextExtra(currentSize, nex.AvgBidByteSize);

        return new Bid
        {
            AuctionId = auction + nex.FirstAuctionId,
            BidderId = bidder + nex.FirstPersonId,
            Price = price,
            DateTime = NexmarkUtils.MilliTsToTimestampString(time),
            Channel = channel,
            Url = url,
            Extra = extra
        };
    }
}
