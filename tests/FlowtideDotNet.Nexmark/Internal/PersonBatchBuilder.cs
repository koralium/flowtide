// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Nexmark.Models;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Nexmark.Internal;

internal sealed class PersonBatchBuilder
{
    private readonly IMemoryAllocator _memoryAllocator;
    private readonly int _batchSize;
    private readonly List<EventBatchData> _batches;
    
    private IColumn[] _currentColumns;
    private int _currentRowCount;

    private static readonly StructHeader AddressHeader = StructHeader.Create("street", "city", "country", "province", "zipcode");
    private static readonly StructHeader ProfileHeader = StructHeader.Create("income", "education", "gender", "business", "age", "interests");

    public PersonBatchBuilder(IMemoryAllocator memoryAllocator, int batchSize, List<EventBatchData> batches)
    {
        _memoryAllocator = memoryAllocator;
        _batchSize = batchSize;
        _batches = batches;
        _currentColumns = CreateColumns();
    }

    private IColumn[] CreateColumns()
    {
        var cols = new IColumn[9];
        for (int i = 0; i < 9; i++)
        {
            cols[i] = Column.Create(_memoryAllocator);
        }
        return cols;
    }

    public void Add(Person person)
    {
        _currentColumns[0].Add(new Int64Value(person.Id));
        _currentColumns[1].Add(new StringValue(person.Name));
        _currentColumns[2].Add(new StringValue(person.EmailAddress));
        
        if (person.Phone != null)
            _currentColumns[3].Add(new StringValue(person.Phone));
        else
            _currentColumns[3].Add(NullValue.Instance);

        if (person.Address != null)
        {
            _currentColumns[4].Add(new StructValue(AddressHeader,
                new StringValue(person.Address.Street),
                new StringValue(person.Address.City),
                new StringValue(person.Address.Country),
                new StringValue(person.Address.Province),
                new StringValue(person.Address.Zipcode)));
        }
        else
        {
            _currentColumns[4].Add(NullValue.Instance);
        }

        if (person.Homepage != null)
            _currentColumns[5].Add(new StringValue(person.Homepage));
        else
            _currentColumns[5].Add(NullValue.Instance);

        if (person.CreditCard != null)
            _currentColumns[6].Add(new StringValue(person.CreditCard));
        else
            _currentColumns[6].Add(NullValue.Instance);

        if (person.Profile != null)
        {
            var p = person.Profile;
            var interestsList = new List<IDataValue>(p.Interests.Count);
            for (int i = 0; i < p.Interests.Count; i++)
            {
                interestsList.Add(new Int64Value(p.Interests[i]));
            }

            _currentColumns[7].Add(new StructValue(ProfileHeader,
                new StringValue(p.Income),
                p.Education != null ? new StringValue(p.Education) : NullValue.Instance,
                p.Gender != null ? new StringValue(p.Gender) : NullValue.Instance,
                new StringValue(p.Business),
                p.Age.HasValue ? new Int64Value(p.Age.Value) : NullValue.Instance,
                new ListValue(interestsList)));
        }
        else
        {
            _currentColumns[7].Add(NullValue.Instance);
        }

        _currentColumns[8].Add(new Int64Value(person.DateTime));

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
}
