using System;
using System.Collections.Generic;

namespace FlowtideDotNet.Nexmark.Internal.Config;

public class NexmarkConfig
{
    public const int ChannelNumber = 10_000;
    public const long NexmarkBaseTime = 1_436_918_400_000;

    public int ActivePeople { get; set; } = 1000;
    public int InFlightAuctions { get; set; } = 100;
    public int OutOfOrderGroupSize { get; set; } = 1;
    public int AvgPersonByteSize { get; set; } = 200;
    public int AvgAuctionByteSize { get; set; } = 500;
    public int AvgBidByteSize { get; set; } = 100;
    public int HotSellerRatio { get; set; } = 4;
    public int HotAuctionRatio { get; set; } = 2;
    public int HotBidderRatio { get; set; } = 4;
    public int HotChannelRatio { get; set; } = 2;
    public long FirstEventId { get; set; } = 0;
    public long FirstEventNumber { get; set; } = 0;
    public long BaseTime { get; set; } = NexmarkBaseTime;
    public long StepLength { get; set; } = 0;
    public long EventsPerEpoch { get; set; } = 0;
    public double EpochPeriod { get; set; } = 0.0;
    public List<double> InterEventDelays { get; set; } = new List<double>();
    public int NumCategories { get; set; } = 5;
    public int AuctionIdLead { get; set; } = 10;
    public int HotSellerRatio2 { get; set; } = 100;
    public int HotAuctionRatio2 { get; set; } = 100;
    public int HotBidderRatio2 { get; set; } = 100;
    public int PersonProportion { get; set; } = 1;
    public int AuctionProportion { get; set; } = 3;
    public int BidProportion { get; set; } = 3;
    public int ProportionDenominator => PersonProportion + AuctionProportion + BidProportion;
    public long FirstAuctionId { get; set; } = 1000;
    public long FirstPersonId { get; set; } = 1000;
    public long FirstCategoryId { get; set; } = 10;
    public int PersonIdLead { get; set; } = 10;
    public int SineApproxSteps { get; set; } = 10;
    
    public string[] UsStates { get; set; } = { "az", "ca", "id", "or", "wa", "wy" };
    public string[] UsCities { get; set; } = { "phoenix", "los angeles", "san francisco", "boise", "portland", "bend", "redmond", "seattle", "kent", "cheyenne" };
    public string[] HotChannels { get; set; } = { "Google", "Facebook", "Baidu", "Apple" };
    public string[] HotUrls { get; set; } = new string[4];
    public string[] FirstNames { get; set; } = { "peter", "paul", "luke", "john", "saul", "vicky", "kate", "julie", "sarah", "deiter", "walter" };
    public string[] LastNames { get; set; } = { "shultz", "abrams", "spencer", "white", "bartels", "walton", "smith", "jones", "noris" };
    
    public Dictionary<int, (string Channel, string Url)> ChannelUrlMap { get; set; } = new();
    public int NumEventGenerators { get; set; } = 1;

    public static NexmarkConfig Default()
    {
        var cfg = new NexmarkConfig();
        
        for(int i=0; i<4; i++) {
            cfg.HotUrls[i] = NexmarkUtils.GetBaseUrl((ulong)i);
        }

        string rateShape = "sine";
        long ratePeriod = 600;
        long firstRate = 10_000;
        long nextRate = 10_000;
        long usPerUnit = 1_000_000;
        int generators = 1;

        double rateToPeriod(long r) => (double)usPerUnit / r;

        if (firstRate == nextRate) {
            cfg.InterEventDelays.Add(rateToPeriod(firstRate) * generators);
        } else {
            if (rateShape == "square") {
                cfg.InterEventDelays.Add(rateToPeriod(firstRate) * generators);
                cfg.InterEventDelays.Add(rateToPeriod(nextRate) * generators);
            } else {
                double mid = (firstRate + nextRate) / 2.0;
                double amp = (firstRate - nextRate) / 2.0;
                for (int i = 0; i < cfg.SineApproxSteps; i++) {
                    double r = (2.0 * Math.PI * i) / cfg.SineApproxSteps;
                    double rate = mid + amp * Math.Cos(r);
                    cfg.InterEventDelays.Add(rateToPeriod((long)Math.Round(rate)) * generators);
                }
            }
        }

        int n = rateShape == "square" ? 2 : cfg.SineApproxSteps;
        cfg.StepLength = (ratePeriod + n - 1) / n;
        
        cfg.EventsPerEpoch = 0;
        cfg.EpochPeriod = 0.0;
        if (cfg.InterEventDelays.Count > 1) {
            foreach (var delay in cfg.InterEventDelays) {
                double numEventsForThisCycle = (cfg.StepLength * 1_000_000.0) / delay;
                cfg.EventsPerEpoch += (long)Math.Round(numEventsForThisCycle);
                cfg.EpochPeriod += (numEventsForThisCycle * delay) / 1000.0;
            }
        }

        cfg.ChannelUrlMap = NexmarkUtils.BuildChannelUrlMap(ChannelNumber);

        return cfg;
    }

    public long EventTimestamp(long eventNumber)
    {
        if (InterEventDelays.Count == 1)
        {
            return BaseTime + (long)Math.Round((eventNumber * InterEventDelays[0]) / 1000.0);
        }

        long epoch = eventNumber / EventsPerEpoch;
        long eventI = eventNumber % EventsPerEpoch;
        double offsetInEpoch = 0.0;
        foreach (var delay in InterEventDelays)
        {
            double numEventsForThisCycle = (StepLength * 1_000_000.0) / delay;
            if (OutOfOrderGroupSize < Math.Round(numEventsForThisCycle))
            {
                double offsetInCycle = eventI * delay;
                return BaseTime + (long)Math.Round(epoch * EpochPeriod + offsetInEpoch + offsetInCycle / 1000.0);
            }
            eventI -= (long)Math.Round(numEventsForThisCycle);
            offsetInEpoch += (numEventsForThisCycle * delay) / 1000.0;
        }
        return 0;
    }

    public long NextAdjustedEvent(long eventsSoFar)
    {
        long n = OutOfOrderGroupSize;
        long eventNumber = FirstEventNumber + eventsSoFar;
        return (eventNumber / n) * n + (eventNumber * 953) % n;
    }
}
