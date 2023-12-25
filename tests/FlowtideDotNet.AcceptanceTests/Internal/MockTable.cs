// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FastMember;
using FlowtideDotNet.Core;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public class RowOperation
    {
        public RowOperation(object obj, bool isDelete)
        {
            IsDelete = isDelete;
            Object = obj;
        }

        public bool IsDelete { get; }

        public object Object { get; }
    }

    public class MockTable
    {

        private readonly object _lock = new object();
        private List<RowOperation> _changes;

        public MockTable(List<string> columns, List<int> primaryKeyIndices)
        {
            Columns = columns;
            PrimaryKeyIndices = primaryKeyIndices;
            _changes = new List<RowOperation>();
        }

        public List<string> Columns { get; }
        public List<int> PrimaryKeyIndices { get; }

        public void AddOrUpdate<T>(IEnumerable<T> rows)
        {
            foreach(var row in rows)
            {
                _changes.Add(new RowOperation(row, false));
            }
        }

        //public void AddOrUpdate(IEnumerable<byte[]> rows)
        //{
        //    lock (_lock)
        //    {
        //        foreach (var row in rows)
        //        {
        //            _changes.Add(new RowOperation(row, false));
        //        }
        //    }
        //}

        public int CurrentOffset => _changes.Count;

        public static RowEvent ToStreamEvent(RowOperation rowOperation, List<string> columns)
        {
            var typeAccessor = TypeAccessor.Create(rowOperation.Object.GetType());

            return RowEvent.Create(rowOperation.IsDelete ? -1 : 1, 0, (b) =>
            {
                foreach (var columnName in columns)
                {
                    var column = typeAccessor[rowOperation.Object, columnName];

                    if (column == null)
                    {
                        b.AddNull();
                    }
                    else if (column is int intVal)
                    {
                        b.Add(intVal);
                    }
                    else if (column is long longVal)
                    {
                        b.Add(longVal);
                    }
                    else if (column is string stringVal)
                    {
                        b.Add(stringVal);
                    }
                    else if (column is double doubleVal)
                    {
                        b.Add(doubleVal);
                    }
                    else if (column is bool boolVal)
                    {
                        b.Add(boolVal);
                    }
                    else if (column is DateTime dateTime)
                    {
                        var ticks = new DateTimeOffset(dateTime).Subtract(DateTimeOffset.UnixEpoch).Ticks;
                        b.Add(ticks);
                    }
                    else if (column.GetType().IsEnum)
                    {
                        b.Add((int)column);
                    }
                    else if (column is Guid guid)
                    {
                        var bytes = guid.ToByteArray();
                        b.Add(bytes);
                    }
                    else if (column is List<int> listInt)
                    {
                        b.Vector(b =>
                        {
                            foreach (var item in listInt)
                            {
                                b.Add(item);
                            }
                        });
                    }
                    else if (column is List<KeyValuePair<string, int>> listKeyInt)
                    {
                        b.Vector(v =>
                        {
                            foreach (var item in listKeyInt)
                            {
                                v.Map(m =>
                                {
                                    m.Add(item.Key, item.Value);
                                });
                            }
                        });
                    }
                    else if (column is List<List<int>> listListInt)
                    {
                        b.Vector(v =>
                        {
                            foreach (var item in listListInt)
                            {
                                v.Vector(m =>
                                {
                                    foreach(var inner in item)
                                    {
                                        m.Add(inner);
                                    }
                                });
                            }
                        });
                    }
                    else if (column is decimal dec)
                    {
                        b.Add(dec);
                    }
                    else
                    {
                        throw new NotImplementedException($"{column.GetType().Name}");
                    }
                }
            });
            
            
        }

        public (IEnumerable<RowOperation> changes, int offset) GetOperations(int fromOffset)
        {
            lock (_lock)
            {
                return (_changes.GetRange(fromOffset, _changes.Count - fromOffset), _changes.Count);
            }
        }
    }
}
