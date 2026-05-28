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
using FlowtideDotNet.Nexmark.Internal.Builders;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Win32.SafeHandles;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Nexmark.Internal;

internal class NexmarkDataSourceState
{
    public int SentBatches { get; set; }
}

public class NexmarkDataSourceOperator : ReadBaseOperator
{
    private static readonly Meter meter = new Meter("FlowtideDotNet.Nexmark", "1.0");
    private readonly ReadRelation _readRelation;
    private readonly string _fileName;
    private readonly List<int> _emitIndices;
    private IObjectState<NexmarkDataSourceState>? _state;
    private readonly HashSet<string> _watermarkNames;
    private readonly Counter<int> _rowCounter;

    public NexmarkDataSourceOperator(ReadRelation readRelation, string fileName, List<int> emitIndices, DataflowBlockOptions options) : base(options)
    {
        _readRelation = readRelation;
        _fileName = fileName;
        _emitIndices = emitIndices;
        _watermarkNames = new HashSet<string>() { readRelation.NamedTable.DotSeperated };
        _rowCounter = meter.CreateCounter<int>("IngressRowCount");
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

        using SafeFileHandle handle = File.OpenHandle(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
        var pipeReader = new FilePipeReader(handle, 16 * 1024 * 1024);

        try
        {
            int currentBatchIndex = 0;

            while (true)
            {
                var result = await pipeReader.ReadAsync();
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    break;
                }

                EventBatchData? batchData = null;
                if (!CheckAndDeserializeBatch(buffer, _emitIndices, out batchData, out int totalLength))
                {
                    pipeReader.AdvanceTo(buffer.Start, buffer.End);
                    continue;
                }

                pipeReader.SkipForward(totalLength);

                if (currentBatchIndex < _state.Value.SentBatches)
                {
                    currentBatchIndex++;
                    continue;
                }

                int rowCount = batchData.Count;

                IColumn[] selectedColumns = new IColumn[_emitIndices.Count];
                for (int i = 0; i < _emitIndices.Count; i++)
                {
                    selectedColumns[i] = batchData.Columns[_emitIndices[i]];
                }

                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

                for (int i = 0; i < rowCount; i++)
                {
                    weights.Add(1);
                    iterations.Add(1);
                }

                var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(selectedColumns)));
                await output.SendAsync(outputBatch);
                
                _state.Value.SentBatches++;
                currentBatchIndex++;
                sentData = true;
            }
        }
        finally
        {
            pipeReader.Complete();
        }

        if (sentData)
        {
            await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.SentBatches)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(200));
        }

        output.ExitCheckpointLock();
    }

    private bool CheckAndDeserializeBatch(
        System.Buffers.ReadOnlySequence<byte> buffer,
        System.Collections.Generic.IReadOnlyList<int> emitIndices,
        out EventBatchData? batchData,
        out int totalLength)
    {
        var sequenceReader = new System.Buffers.SequenceReader<byte>(buffer);
        var deserializer = new Core.ColumnStore.Serialization.EventBatchDeserializer(MemoryAllocator);

        if (!deserializer.HasEnoughBytesForProjection(ref sequenceReader, emitIndices))
        {
            batchData = null;
            totalLength = 0;
            return false;
        }

        sequenceReader = new System.Buffers.SequenceReader<byte>(buffer);
        try
        {
            var deserializeResult = deserializer.DeserializeBatch(ref sequenceReader, emitIndices);
            batchData = deserializeResult.EventBatch;
            totalLength = deserializeResult.TotalLength;
            return true;
        }
        catch (Exception e) when (e.Message.Contains("Not enough data") || e.Message.Contains("Failed to read"))
        {
            batchData = null;
            totalLength = 0;
            return false;
        }
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

        using SafeFileHandle handle = File.OpenHandle(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
        var pipeReader = new FilePipeReader(handle, 4 * 1024 * 1024);

        int currentBatchIndex = 0;
        int totalRowCount = 0;
        while (true)
        {
            var result = await pipeReader.ReadAsync();
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                break;
            }

            EventBatchData? batchData = null;
            if (!CheckAndDeserializeBatch(buffer, _emitIndices, out batchData, out int totalLength))
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            pipeReader.SkipForward(totalLength);

            if (currentBatchIndex < _state.Value.SentBatches)
            {
                currentBatchIndex++;
                continue;
            }

            int rowCount = batchData.Count;

            totalRowCount += rowCount;

            IColumn[] selectedColumns = new IColumn[_emitIndices.Count];
            for (int i = 0; i < _emitIndices.Count; i++)
            {
                selectedColumns[i] = batchData.Columns[_emitIndices[i]];
            }

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            for (int i = 0; i < rowCount; i++)
            {
                weights.Add(1);
                iterations.Add(1);
            }

            var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(selectedColumns)));
            await output.SendAsync(outputBatch);

            _state.Value.SentBatches++;
            currentBatchIndex++;
            sentData = true;
        }

        _rowCounter.Add(totalRowCount, new KeyValuePair<string, object?>("operator", Name));
        if (sentData)
        {
            await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.SentBatches)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }

        output.ExitCheckpointLock();
    }
}
