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

using FlexBuffers;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal.StatefulAggregations
{
    internal class MinAggregationInsertComparer : IComparer<RowEvent>
    {
        private readonly int length;

        public MinAggregationInsertComparer(int length)
        {
            this.length = length;
        }
        public int Compare(RowEvent x, RowEvent y)
        {
            for (int i = 0; i < length; i++)
            {
                var c = FlxValueRefComparer.CompareTo(x.GetColumnRef(i), y.GetColumnRef(i));
                if (c != 0)
                {
                    return c;
                }
            }
            return 0;
        }
    }

    internal class MaxAggregationInsertComparer : IComparer<RowEvent>
    {
        private readonly int length;

        public MaxAggregationInsertComparer(int length)
        {
            this.length = length;
        }

        public int Compare(RowEvent x, RowEvent y)
        {
            for (int i = 0; i < length; i++)
            {
                var yref = y.GetColumnRef(i);
                var xref = x.GetColumnRef(i);

                if (yref.IsNull)
                {
                    if (xref.IsNull)
                    {
                        continue;
                    }
                    else
                    {
                        return 1;
                    }
                }
                else if (xref.IsNull)
                {
                    return -1;
                }

                var c = FlxValueRefComparer.CompareTo(yref, xref);
                if (c != 0)
                {
                    return c;
                }
            }
            return 0;
        }
    }

    internal class MinMaxAggregationSingleton
    {
        private readonly int keyLength;

        public MinMaxAggregationSingleton(IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>> tree, int keyLength)
        {
            Tree = tree;
            this.keyLength = keyLength;
        }

        public int KeyLength => keyLength;
        public IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>> Tree { get; }
        public bool AreKeyEqual(RowEvent x, RowEvent y)
        {
            for (int i = 0; i < keyLength; i++)
            {
                var c = FlxValueRefComparer.CompareTo(x.GetColumnRef(i), y.GetColumnRef(i));
                if (c != 0)
                {
                    return false;
                }
            }
            return true;
        }
    }
    internal static class MinMaxAggregationRegistration
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());

        private static async Task<MinMaxAggregationSingleton> InitializeMinMax(
            int groupingLength, 
            IStateManagerClient stateManagerClient, 
            IComparer<RowEvent> comparer,
            string treeName)
        {
            List<int> insertPrimaryKeys = new List<int>();
            for (int i = 0; i < groupingLength + 1; i++)
            {
                insertPrimaryKeys.Add(i);
            }
            List<int> searchPrimaryKeys = new List<int>();
            for (int i = 0; i < groupingLength; i++)
            {
                searchPrimaryKeys.Add(i);
            }
            var tree = await stateManagerClient.GetOrCreateTree(treeName, 
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
            {
                Comparer = new BPlusTreeListComparer<RowEvent>(comparer),
                KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            });

            return new MinMaxAggregationSingleton(tree, groupingLength);
        }

        private static Task<MinMaxAggregationSingleton> InitializeMin(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return InitializeMinMax(groupingLength, stateManagerClient, new MinAggregationInsertComparer(groupingLength + 1), "mintree");
        }

        private static Task<MinMaxAggregationSingleton> InitializeMax(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return InitializeMinMax(groupingLength, stateManagerClient, new MaxAggregationInsertComparer(groupingLength + 1), "maxtree");
        }



        private static System.Linq.Expressions.Expression MinMaxMapFunction(
            Substrait.Expressions.AggregateFunction function,
            ParametersInfo parametersInfo,
            ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
            ParameterExpression stateParameter,
            ParameterExpression weightParameter,
            ParameterExpression singletonAccess,
            ParameterExpression groupingKeyParameter)
        {
            if (function.Arguments.Count != 1)
            {
                throw new InvalidOperationException("Min must have one argument.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            var expr = GetMinMaxBody();
            var body = expr.Body;
            var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
            System.Linq.Expressions.Expression e = replacer.Visit(body);
            replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[3], singletonAccess);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[4], groupingKeyParameter);
            e = replacer.Visit(e);
            return e;
        }

        private static async ValueTask<FlxValue> MinMaxGetValue(byte[]? state, RowEvent groupingKey, MinMaxAggregationSingleton singleton)
        {
            var vector = FlexBufferBuilder.Vector(v =>
            {
                for (int i = 0; i < groupingKey.Length; i++)
                {
                    v.Add(groupingKey.GetColumn(i));
                }
                v.AddNull();
            });
            var row = new RowEvent(0, 0, new CompactRowData(vector, FlxValue.FromMemory(vector).AsVector));
            var iterator = singleton.Tree.CreateIterator();
            await iterator.Seek(row);
            
            await foreach(var page in iterator)
            {
                foreach(var kv in page)
                {
                    if (singleton.AreKeyEqual(kv.Key, row))
                    {
                        return kv.Key.GetColumn(singleton.KeyLength);
                    }
                    else
                    {
                        return NullValue;
                    }
                }
            }
            return NullValue;
        }

        private static Expression<Func<FlxValue, byte[], long, MinMaxAggregationSingleton, RowEvent, ValueTask<byte[]>>> GetMinMaxBody()
        {
            return (ev, bytes, weight, singleton, groupingKey) => DoMinMax(ev, bytes, weight, singleton, groupingKey);
        }

        private static async ValueTask<byte[]> DoMinMax(FlxValue column, byte[] currentState, long weight, MinMaxAggregationSingleton singleton, RowEvent groupingKey)
        {
            if (column.IsNull)
            {
                return currentState;
            }
            var vector = FlexBufferBuilder.Vector(v =>
            {
                for (int i = 0; i < groupingKey.Length; i++)
                {
                    v.Add(groupingKey.GetColumn(i));
                }
                v.Add(column);
            });
            var row = new RowEvent((int)weight, 0, new CompactRowData(vector, FlxValue.FromMemory(vector).AsVector));
            await singleton.Tree.RMW(row, (int)weight, (input, current, exists) =>
            {
                if (exists)
                {
                    current += input;

                    if (current == 0)
                    {
                        return (0, GenericWriteOperation.Delete);
                    }
                    return (current, GenericWriteOperation.Upsert);
                }
                return (input, GenericWriteOperation.Upsert);
            });

            return currentState;
        }

        private static async Task Commit(MinMaxAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        public static void RegisterMax(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulAggregateFunction<MinMaxAggregationSingleton>(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.Max,
                InitializeMax,
                (singleton) => { },
                Commit,
                MinMaxMapFunction,
                MinMaxGetValue
                );
        }

        public static void RegisterMin(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulAggregateFunction<MinMaxAggregationSingleton>(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.Min,
                InitializeMin,
                (singleton) => { },
                Commit,
                MinMaxMapFunction,
                MinMaxGetValue
                );
        }
    }
}
