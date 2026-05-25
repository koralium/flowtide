using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;

namespace FlowtideDotNet.Nexmark.Internal.Builders;

internal sealed class PersonBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private IColumn[] _currentColumns;
    private int _currentRowCount;
    private readonly EventBatchSerializer _eventBatchSerializer;
    private readonly FileStream _fileStream;

    public int EventCount { get; private set; }

    public PersonBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize, string baseDir)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _currentColumns = CreateColumns();
        _currentRowCount = 0;
        _eventBatchSerializer = new EventBatchSerializer();
        string filePath = Path.Combine(baseDir, "person_batches.bin");
        _fileStream = File.OpenWrite(filePath);
    }

    public void Add(in Person person)
    {
        _currentColumns[0].Add(new Int64Value(person.Id));
        _currentColumns[1].Add(new StringValue(person.Name));
        _currentColumns[2].Add(new StringValue(person.EmailAddress));
        _currentColumns[3].Add(new StringValue(person.CreditCard));
        _currentColumns[4].Add(new StringValue(person.City));
        _currentColumns[5].Add(new StringValue(person.State));
        _currentColumns[6].Add(new StringValue(person.DateTime));
        _currentColumns[7].Add(new StringValue(person.Extra));

        _currentRowCount++;
        EventCount++;

        if (_currentRowCount >= _batchSize)
        {
            Flush();
        }
    }

    public void Flush()
    {
        if (_currentRowCount > 0)
        {
            var b = new EventBatchData(_currentColumns);
            var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
            _eventBatchSerializer.SerializeEventBatch(bufferWriter, b, b.Count);
            
            var span = bufferWriter.WrittenSpan;
            _fileStream.Write(span);
            
            _currentColumns = CreateColumns();
            _currentRowCount = 0;
        }
    }

    public void Complete()
    {
        Flush();
        _fileStream.Dispose();
    }

    private IColumn[] CreateColumns()
    {
        var columns = new IColumn[8];
        for (int i = 0; i < 8; i++)
        {
            columns[i] = Column.Create(_memoryAllocator);
        }
        return columns;
    }
}
