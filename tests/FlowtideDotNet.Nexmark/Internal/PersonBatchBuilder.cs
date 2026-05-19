using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Generic;

namespace FlowtideDotNet.Nexmark.Internal;

internal sealed class PersonBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private readonly List<EventBatchData> _batches;
    private IColumn[] _currentColumns;
    private int _currentRowCount;

    public PersonBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize, List<EventBatchData> batches)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _batches = batches;
        _currentColumns = CreateColumns();
        _currentRowCount = 0;
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

        if (_currentRowCount >= _batchSize)
        {
            Flush();
        }
    }

    public void Flush()
    {
        if (_currentRowCount > 0)
        {
            _batches.Add(new EventBatchData(_currentColumns));
            _currentColumns = CreateColumns();
            _currentRowCount = 0;
        }
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
