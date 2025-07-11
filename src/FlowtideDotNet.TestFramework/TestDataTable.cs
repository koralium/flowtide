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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.TestFramework
{
    public abstract class TestDataTable
    {
        public abstract void AddRows<T>(params T[] rows);

        public abstract void RemoveRows<T>(params T[] rows);

        internal abstract bool TryGetNextBatch(int index, [NotNullWhen(true)] out EventBatchWeighted? batch);

        internal abstract NamedStruct GetSchema();

        public static TestDataTable Create<T>()
        {
            return new TestDataTable<T>();
        }

        public static TestDataTable Create<T>(params T[] rows)
        {
            var table = new TestDataTable<T>();
            table.AddRows(rows);
            return table;
        }
    }
    public class TestDataTable<T> : TestDataTable
    {
        private Type? _tableType;
        private BatchConverter? _converter;
        private List<EventBatchWeighted> outputBatches = new List<EventBatchWeighted>();
        private readonly NamedStruct _schema;
        private readonly object _lock = new object();
        public TestDataTable()
        {
            _tableType = typeof(T);
            _converter = BatchConverter.GetBatchConverter(_tableType);

            List<string> names = new List<string>();
            var types = new List<SubstraitBaseType>();
            for (int i = 0; i < _converter.Properties.Count; i++)
            {
                names.Add(_converter.Properties[i].Name);
                types.Add(GetSubstraitType(_converter.Properties[i].TypeInfo.Type));
            }
            _schema = new NamedStruct()
            {
                Names = names,
                Struct = new Struct()
                {
                    Types = types
                }
            };
        }


        private static SubstraitBaseType GetSubstraitType(Type type)
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
                return new AnyType();
            }
        }

        public IReadOnlyList<EventBatchWeighted> OutputBatches => outputBatches;
        
        public override void AddRows<TType>(params TType[] rows)
        {
            if (_tableType != typeof(TType))
            {
                throw new InvalidOperationException("Cannot mix types in a single table");
            }
            var batch = _converter!.ConvertToEventBatch(rows, GlobalMemoryManager.Instance);
            PrimitiveList<int> weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            weights.InsertStaticRange(0, 1, batch.Count);
            iterations.InsertStaticRange(0, 0, batch.Count);

            lock (_lock)
            {
                outputBatches.Add(new EventBatchWeighted(weights, iterations, batch));
            }
        }

        internal override bool TryGetNextBatch(int index, [NotNullWhen(true)] out EventBatchWeighted? batch)
        {
            lock (_lock)
            {
                if (outputBatches.Count > index)
                {
                    batch = outputBatches[index];
                    return true;
                }
                else
                {
                    batch = default;
                    return false;
                }
            }
        }

        internal override NamedStruct GetSchema()
        {
            return _schema;
        }

        public override void RemoveRows<TType>(params TType[] rows)
        {
            if (_tableType != typeof(TType))
            {
                throw new InvalidOperationException("Cannot mix types in a single table");
            }
            var batch = _converter!.ConvertToEventBatch(rows, GlobalMemoryManager.Instance);
            PrimitiveList<int> weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            weights.InsertStaticRange(0, -1, batch.Count);
            iterations.InsertStaticRange(0, 0, batch.Count);

            lock (_lock)
            {
                outputBatches.Add(new EventBatchWeighted(weights, iterations, batch));
            }
        }
    }
}
