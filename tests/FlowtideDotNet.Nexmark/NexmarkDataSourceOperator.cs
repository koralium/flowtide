// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Connector.Files.Internal.CsvFiles.Parser;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Win32.SafeHandles;
using System.Buffers;
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
    private readonly string _fileName;
    private readonly List<int> _emitIndices;
    private IObjectState<NexmarkDataSourceState>? _state;
    private HashSet<string> _watermarkNames;

    public NexmarkDataSourceOperator(ReadRelation readRelation, string fileName, List<int> emitIndices, DataflowBlockOptions options) : base(options)
    {
        _readRelation = readRelation;
        _fileName = fileName;
        _emitIndices = emitIndices;
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

        using var fileStream = System.IO.File.OpenRead(_fileName);
        var pipeReader = System.IO.Pipelines.PipeReader.Create(fileStream);

        int currentBatchIndex = 0;

        while (true)
        {
            var result = await pipeReader.ReadAsync();
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                break;
            }

            if (buffer.Length < 4)
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            int length = GetMessageLength(buffer);

            if (buffer.Length < length + 4)
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            var batchBuffer = buffer.Slice(4, length);
            FlowtideDotNet.Core.ColumnStore.EventBatchData? batchData = null;

            if (!TryDeserializeBatch(batchBuffer, out batchData))
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            pipeReader.AdvanceTo(buffer.GetPosition(length + 4));

            if (currentBatchIndex < _state.Value.SentBatches)
            {
                currentBatchIndex++;
                continue;
            }

            int rowCount = batchData.Count;

            IColumn[] selectedColumns = new IColumn[_emitIndices.Count];
            for (int i = 0; i < _emitIndices.Count; i++)
            {
                selectedColumns[i] = batchData.Columns[_emitIndices[i]].Copy(MemoryAllocator);
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

        if (sentData)
        {
            await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.SentBatches)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(200));
        }

        output.ExitCheckpointLock();
    }

    private int GetMessageLength(System.Buffers.ReadOnlySequence<byte> buffer)
    {
        var reader = new System.Buffers.SequenceReader<byte>(buffer);
        reader.TryReadLittleEndian(out int length);
        return length;
    }

    private bool TryDeserializeBatch(System.Buffers.ReadOnlySequence<byte> buffer, out FlowtideDotNet.Core.ColumnStore.EventBatchData? batchData)
    {
        var deserializer = new FlowtideDotNet.Core.ColumnStore.Serialization.EventBatchDeserializer(MemoryAllocator);
        try
        {
            var sequenceReader = new System.Buffers.SequenceReader<byte>(buffer);
            var deserializeResult = deserializer.DeserializeBatch(ref sequenceReader);
            batchData = deserializeResult.EventBatch;
            return true;
        }
        catch (Exception e) when (e.Message.Contains("Not enough data") || e.Message.Contains("Failed to read"))
        {
            batchData = null;
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
        var pipeReader = new FilePipeReader(handle, 16 * 1024 * 1024);

        int currentBatchIndex = 0;

        while (true)
        {
            var result = await pipeReader.ReadAsync();
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                break;
            }

            if (buffer.Length < 4)
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            int length = GetMessageLength(buffer);

            if (buffer.Length < length + 4)
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            var batchBuffer = buffer.Slice(4, length);
            FlowtideDotNet.Core.ColumnStore.EventBatchData? batchData = null;

            if (!TryDeserializeBatch(batchBuffer, out batchData))
            {
                pipeReader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            pipeReader.AdvanceTo(buffer.GetPosition(length + 4));

            if (currentBatchIndex < _state.Value.SentBatches)
            {
                currentBatchIndex++;
                continue;
            }

            int rowCount = batchData.Count;

            IColumn[] selectedColumns = new IColumn[_emitIndices.Count];
            for (int i = 0; i < _emitIndices.Count; i++)
            {
                selectedColumns[i] = batchData.Columns[_emitIndices[i]]; //.Copy(MemoryAllocator);
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

        if (sentData)
        {
            await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.SentBatches)));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }

        output.ExitCheckpointLock();
        
        // Register changes trigger in case more data is appended later
        //await this.RegisterTrigger("changes", TimeSpan.FromMilliseconds(50));
    }
}
