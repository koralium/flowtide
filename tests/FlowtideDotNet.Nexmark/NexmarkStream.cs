// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Nexmark;

public sealed class NexmarkStream
{
    public required NamedStruct PersonSchema { get; init; }
    public required List<EventBatchData> PersonBatches { get; init; }

    public required NamedStruct AuctionSchema { get; init; }
    public required List<EventBatchData> AuctionBatches { get; init; }

    public required NamedStruct BidSchema { get; init; }
    public required List<EventBatchData> BidBatches { get; init; }
}
