// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Nexmark;

public class NexmarkSourceFactory : RegexConnectorSourceFactory
{
    private readonly NexmarkStream _stream;

    public NexmarkSourceFactory(string regexPattern, NexmarkStream stream) : base(regexPattern)
    {
        _stream = stream;
    }

    public override Relation ModifyPlan(ReadRelation readRelation)
    {
        return readRelation;
    }

    public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
    {
        var tableName = readRelation.NamedTable.DotSeperated.ToLowerInvariant();
        List<FlowtideDotNet.Core.ColumnStore.EventBatchData>? batches = null;

        if (tableName.Contains("person"))
        {
            batches = _stream.PersonBatches;
        }
        else if (tableName.Contains("auction"))
        {
            batches = _stream.AuctionBatches;
        }
        else if (tableName.Contains("bid"))
        {
            batches = _stream.BidBatches;
        }
        else
        {
            throw new ArgumentException($"Unknown table name: {tableName}");
        }

        return new NexmarkDataSourceOperator(readRelation, batches, dataflowBlockOptions);
    }

    public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
    {
        return new TableLineageMetadata("nexmark", readRelation.NamedTable.DotSeperated, default);
    }
}
