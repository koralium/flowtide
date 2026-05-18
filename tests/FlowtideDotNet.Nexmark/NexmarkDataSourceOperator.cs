// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Nexmark;

internal class NexmarkDataSourceState
{
    public int SentBatches { get; set; }
}

public class NexmarkDataSourceOperator : ReadBaseOperator
{
    private readonly ReadRelation _readRelation;
    private readonly List<EventBatchData> _batches;
    private IObjectState<NexmarkDataSourceState>? _state;
    private HashSet<string> _watermarkNames;

    public NexmarkDataSourceOperator(ReadRelation readRelation, List<EventBatchData> batches, DataflowBlockOptions options) : base(options)
    {
        _readRelation = readRelation;
        _batches = batches;
        _watermarkNames = new HashSet<string>() { readRelation.NamedTable.DotSeperated };
    }

    public override string DisplayName => $"Nexmark Data Source ({_readRelation.NamedTable.DotSeperated})";

    public override Task DeleteAsync()
    {
        return Task.CompletedTask;
    }

    public override Task OnTrigger(string triggerName, object? state)
    {
        if (triggerName == "changes")
        {
            RunTask(FetchChanges);
        }
        return Task.CompletedTask;
    }

    private async Task FetchChanges(IngressOutput<StreamEventBatch> output, object? state)
    {
        Debug.Assert(_state?.Value != null);
        await output.EnterCheckpointLock();
        
        bool sentData = false;

        while (_state.Value.SentBatches < _batches.Count)
        {
            var batchData = _batches[_state.Value.SentBatches];
            int rowCount = batchData.Count;

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            // Add weights and iterations for all rows in this batch
            // The primitive lists will grow automatically
            for (int i = 0; i < rowCount; i++)
            {
                weights.Add(1);
                iterations.Add(1);
            }

            var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, batchData));
            await output.SendAsync(outputBatch);
            
            _state.Value.SentBatches++;
            sentData = true;
        }

        if (sentData)
        {
            await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.SentBatches)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(200));
        }

        output.ExitCheckpointLock();
    }

    protected override Task<IReadOnlySet<string>> GetWatermarkNames()
    {
        return Task.FromResult<IReadOnlySet<string>>(_watermarkNames);
    }

    protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
    {
        _state = await stateManagerClient.GetOrCreateObjectStateAsync<NexmarkDataSourceState>("nexmark_data_source_state_" + _readRelation.NamedTable.DotSeperated);
        if (_state.Value == null)
        {
            _state.Value = new NexmarkDataSourceState();
        }
    }

    protected override async Task OnCheckpoint(long checkpointTime)
    {
        Debug.Assert(_state?.Value != null);
        await _state.Commit();
    }

    protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
    {
        Debug.Assert(_state?.Value != null);
        await output.EnterCheckpointLock();

        bool sentData = false;

        while (_state.Value.SentBatches < _batches.Count)
        {
            var batchData = _batches[_state.Value.SentBatches];
            int rowCount = batchData.Count;

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            for (int i = 0; i < rowCount; i++)
            {
                weights.Add(1);
                iterations.Add(1);
            }

            var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, batchData));
            await output.SendAsync(outputBatch);

            _state.Value.SentBatches++;
            sentData = true;
        }

        if (sentData)
        {
            await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.SentBatches)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }

        output.ExitCheckpointLock();
        
        // Register changes trigger in case more data is appended to _batches later
        await this.RegisterTrigger("changes", TimeSpan.FromMilliseconds(50));
    }
}
