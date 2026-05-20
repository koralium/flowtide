using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Nexmark.Config;
using FlowtideDotNet.Nexmark.Internal;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;
using System.IO.Pipelines;

namespace FlowtideDotNet.Nexmark;

/// <summary>
/// A C# port of the modern stateless Nexmark generator (similar to Apache Beam / RisingWave).
/// Produces a deterministic stream of EventBatchData arrays.
/// </summary>
public sealed class NexmarkGenerator
{
    private readonly long _numGenCalls;
    private readonly int _batchSize;
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly NexmarkConfig _config;

    /// <summary>
    /// Initializes a new instance of the NexmarkGenerator.
    /// </summary>
    /// <param name="genCalls">The number of generation loops to run. Defaults to 1000.</param>
    /// <param name="seed">The random seed/start offset to use. Defaults to 103984.</param>
    /// <param name="batchSize">The maximum number of rows per EventBatchData list. Defaults to 1000.</param>
    /// <param name="memoryAllocator">Memory allocator for columnar data. Uses GlobalMemoryManager if null.</param>
    public NexmarkGenerator(int genCalls = 1000, int seed = 103984, int batchSize = 1000, IMemoryAllocator? memoryAllocator = null)
    {
        _numGenCalls = genCalls;
        _batchSize = batchSize;
        _memoryAllocator = memoryAllocator ?? GlobalMemoryManager.Instance;
        
        _config = NexmarkConfig.Default();
        _config.FirstEventId = seed;
    }

    public NexmarkDataStream Generate()
    {
        var personBuilder = new PersonBatchBuilder(_memoryAllocator, _batchSize);
        var auctionBuilder = new AuctionBatchBuilder(_memoryAllocator, _batchSize);
        var bidBuilder = new BidBatchBuilder(_memoryAllocator, _batchSize);

        for (long eventsSoFar = 0; eventsSoFar < _numGenCalls; eventsSoFar++)
        {
            long rem = _config.NextAdjustedEvent(eventsSoFar) % _config.ProportionDenominator;
            long timestamp = _config.EventTimestamp(_config.NextAdjustedEvent(eventsSoFar));
            long eventId = _config.FirstEventId + _config.NextAdjustedEvent(eventsSoFar);

            if (rem < _config.PersonProportion)
            {
                var person = Person.Generate(eventId, timestamp, _config);
                personBuilder.Add(in person);
            }
            else if (rem < _config.PersonProportion + _config.AuctionProportion)
            {
                var auction = Auction.Generate(eventsSoFar, eventId, timestamp, _config);
                auctionBuilder.Add(in auction);
            }
            else
            {
                var bid = Bid.Generate(eventId, timestamp, _config);
                bidBuilder.Add(in bid);
            }
        }
        
        personBuilder.Flush();
        auctionBuilder.Flush();
        bidBuilder.Flush();

        personBuilder.Complete();
        auctionBuilder.Complete();
        bidBuilder.Complete();

        return new NexmarkDataStream
        {
            PersonSchema = NexmarkSchema.PersonSchema,
            NumberOfPersons = personBuilder.EventCount,
            AuctionSchema = NexmarkSchema.AuctionSchema,
            NumberOfAuctions = auctionBuilder.EventCount,
            BidSchema = NexmarkSchema.BidSchema,
            NumberOfBids = bidBuilder.EventCount
        };
    }
}
