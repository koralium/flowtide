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
using FlowtideDotNet.Substrait.Sql;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Nexmark;

public class NexmarkSourceFactory : RegexConnectorSourceFactory, ITableProvider, IConnectorTableProviderFactory
{
    private readonly NexmarkDataStream _stream;

    public NexmarkSourceFactory(string regexPattern, NexmarkDataStream stream) : base(regexPattern)
    {
        _stream = stream;
    }

    public override Relation ModifyPlan(ReadRelation readRelation)
    {
        if (readRelation.Filter != null)
        {
            return new FilterRelation()
            {
                Input = readRelation,
                Emit = readRelation.Emit,
                Condition = readRelation.Filter,
            };
        }
        return readRelation;
    }

    public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
    {
        var tableName = readRelation.NamedTable.DotSeperated.ToLowerInvariant();
        string fileName = "";
        FlowtideDotNet.Substrait.Type.NamedStruct? schema = null;

        if (tableName.Contains("person"))
        {
            fileName = "person_batches.bin";
            schema = NexmarkSchema.PersonSchema;
        }
        else if (tableName.Contains("auction"))
        {
            fileName = "auction_batches.bin";
            schema = NexmarkSchema.AuctionSchema;
        }
        else if (tableName.Contains("bid"))
        {
            fileName = "bid_batches.bin";
            schema = NexmarkSchema.BidSchema;
        }
        else
        {
            throw new ArgumentException($"Unknown table name: {tableName}");
        }

        List<int> emitIndices = new List<int>();

        for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
        {
            int baseSchemaIndex = i;
            string colName = readRelation.BaseSchema.Names[baseSchemaIndex];
            
            int nexmarkIndex = schema.Names.FindIndex(x => string.Equals(x, colName, StringComparison.OrdinalIgnoreCase));
            if (nexmarkIndex == -1)
            {
                throw new InvalidOperationException($"Column '{colName}' not found in Nexmark schema for {tableName}");
            }
            emitIndices.Add(nexmarkIndex);
        }

        return new NexmarkDataSourceOperator(readRelation, fileName, emitIndices, dataflowBlockOptions);
    }

    public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
    {
        return new TableLineageMetadata("nexmark", readRelation.NamedTable.DotSeperated, default);
    }

    public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
    {
        var dotSeparated = string.Join(".", tableName).ToLowerInvariant();

        if (dotSeparated.Contains("person"))
        {
            tableMetadata = new TableMetadata(dotSeparated, NexmarkSchema.PersonSchema);
            return true;
        }
        else if (dotSeparated.Contains("auction"))
        {
            tableMetadata = new TableMetadata(dotSeparated, NexmarkSchema.AuctionSchema);
            return true;
        }
        else if (dotSeparated.Contains("bid"))
        {
            tableMetadata = new TableMetadata(dotSeparated, NexmarkSchema.BidSchema);
            return true;
        }

        tableMetadata = null;
        return false;
    }

    public bool TryHandleTableFunction(IReadOnlyList<string> functionName, TableProviderTableFunctionArguments sqlTableFunction, [NotNullWhen(true)] out TableProviderTableFunctionResult? relation)
    {
        relation = null;
        return false;
    }

    public ITableProvider Create()
    {
        return this;
    }
}
