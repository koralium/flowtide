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

        public static StreamEvent ToStreamEvent(RowOperation rowOperation, List<string> columns)
        {
            var typeAccessor = TypeAccessor.Create(rowOperation.Object.GetType());

            return StreamEvent.Create(rowOperation.IsDelete ? -1 : 1, 0, (b) =>
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
                    else if (column is string stringVal)
                    {
                        b.Add(stringVal);
                    }
                    else if (column is double doubleVal)
                    {
                        b.Add(doubleVal);
                    }
                    else if (column.GetType().IsEnum)
                    {
                        b.Add((int)column);
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
