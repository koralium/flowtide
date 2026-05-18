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
    private readonly NexmarkDataStream _stream;

    public NexmarkSourceFactory(string regexPattern, NexmarkDataStream stream) : base(regexPattern)
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
        FlowtideDotNet.Substrait.Type.NamedStruct? schema = null;

        if (tableName.Contains("person"))
        {
            batches = _stream.PersonBatches;
            schema = NexmarkSchema.PersonSchema;
        }
        else if (tableName.Contains("auction"))
        {
            batches = _stream.AuctionBatches;
            schema = NexmarkSchema.AuctionSchema;
        }
        else if (tableName.Contains("bid"))
        {
            batches = _stream.BidBatches;
            schema = NexmarkSchema.BidSchema;
        }
        else
        {
            throw new ArgumentException($"Unknown table name: {tableName}");
        }

        List<int> emitIndices = new List<int>();
        var emitList = readRelation.Emit;
        if (emitList == null)
        {
            emitList = Enumerable.Range(0, readRelation.BaseSchema.Names.Count).ToList();
        }

        for (int i = 0; i < emitList.Count; i++)
        {
            int baseSchemaIndex = emitList[i];
            string colName = readRelation.BaseSchema.Names[baseSchemaIndex];
            
            int nexmarkIndex = schema.Names.FindIndex(x => string.Equals(x, colName, StringComparison.OrdinalIgnoreCase));
            if (nexmarkIndex == -1)
            {
                throw new InvalidOperationException($"Column '{colName}' not found in Nexmark schema for {tableName}");
            }
            emitIndices.Add(nexmarkIndex);
        }

        return new NexmarkDataSourceOperator(readRelation, batches, emitIndices, dataflowBlockOptions);
    }

    public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
    {
        return new TableLineageMetadata("nexmark", readRelation.NamedTable.DotSeperated, default);
    }
}
