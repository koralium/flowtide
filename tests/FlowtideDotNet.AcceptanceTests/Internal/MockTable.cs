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
using FlowtideDotNet.Substrait.Type;

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
        public Type _type;

        public MockTable(List<string> columns, List<int> primaryKeyIndices, List<SubstraitBaseType> types, Type type)
        {
            Columns = columns;
            PrimaryKeyIndices = primaryKeyIndices;
            Types = types;
            _type = type;
            _changes = new List<RowOperation>();
        }

        public Type Type => _type;
        public List<string> Columns { get; }
        public List<int> PrimaryKeyIndices { get; }
        public List<SubstraitBaseType> Types { get; }

        public void AddOrUpdate<T>(IEnumerable<T> rows)
        {
            foreach(var row in rows)
            {
                if (row == null)
                {
                    throw new InvalidOperationException("Cannot add null row");
                }
                _changes.Add(new RowOperation(row, false));
            }
        }

        public void Delete<T>(IEnumerable<T> rows)
        {
            foreach(var row in rows)
            {
                if (row == null)
                {
                    throw new InvalidOperationException("Cannot delete null row");
                }
                _changes.Add(new RowOperation(row, true));
            }
        }

        public int CurrentOffset => _changes.Count;

        public static SubstraitBaseType GetSubstraitType(Type type)
        {
            if (type.IsEnum)
            {
                return new Int64Type();
            }
            if (type == typeof(int))
            {
                return new Int64Type();
            }
            else if (type == typeof(long))
            {
                return new Int64Type();
            }
            else if (type == typeof(string))
            {
                return new StringType();
            }
            else if (type == typeof(double))
            {
                return new Fp64Type();
            }
            else if (type == typeof(bool))
            {
                return new BoolType();
            }
            else if (type == typeof(DateTime))
            {
                return new Int64Type();
            }
            else if (type == typeof(Guid))
            {
                return new StringType();
            }
            else if (type == typeof(List<int>))
            {
                return new ListType(new Int64Type());
            }
            else if (type == typeof(List<KeyValuePair<string, int>>))
            {
                return new ListType(new MapType(new StringType(), new Int64Type()));
            }
            else if (type == typeof(List<List<int>>))
            {
                return new ListType(new ListType(new Int64Type()));
            }
            else if (type == typeof(decimal))
            {
                return new DecimalType();
            }
            else if (type.IsGenericType &&
                type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var innerType = GetSubstraitType(type.GetGenericArguments()[0]);
                innerType.Nullable = true;
                return innerType;
            }
            else
            {
                throw new NotImplementedException($"{type.Name}");
            }
        }

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
                        var guidString = guid.ToString();
                        b.Add(guidString);
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
                    else if (column is List<string> listString)
                    {
                        b.Vector(b =>
                        {
                            foreach (var item in listString)
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
                    else if (column is List<KeyValuePair<string, object>[]> listKeyValArr)
                    {
                        b.Vector(v =>
                        {
                            foreach (var item in listKeyValArr)
                            {
                                v.Map(m =>
                                {
                                    foreach (var kv in item)
                                    {
                                        if (kv.Value == null)
                                        {
                                            m.AddNull(kv.Key);
                                        }
                                        else if (kv.Value is int intV)
                                        {
                                            m.Add(kv.Key, intV);
                                        }
                                        else if (kv.Value is string stringV)
                                        {
                                            m.Add(kv.Key, stringV);
                                        }   
                                    }
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
                    else if (column is DateTimeOffset dateTimeOffset)
                    {
                        b.Add(dateTimeOffset.Subtract(DateTimeOffset.UnixEpoch).Ticks);
                    }
                    else if (column is byte[] bytes)
                    {
                        b.Add(bytes);
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
