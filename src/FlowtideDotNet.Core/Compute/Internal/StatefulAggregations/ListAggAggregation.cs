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
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Buffers;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal.StatefulAggregations
{
    internal class ListAggAggregationInsertComparer : IComparer<RowEvent>
    {
        private readonly int length;

        public ListAggAggregationInsertComparer(int length)
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

    internal class ListAggAggregationSingleton
    {
        private readonly int keyLength;

        public ListAggAggregationSingleton(IBPlusTree<RowEvent, int> tree, int keyLength)
        {
            Tree = tree;
            this.keyLength = keyLength;
            OutputBuilder = new FlexBuffer(ArrayPool<byte>.Shared);
        }
        public FlexBuffer OutputBuilder { get; } 
        public int KeyLength => keyLength;
        public IBPlusTree<RowEvent, int> Tree { get; }
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
    internal static class ListAggAggregation
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());

        private static async Task<ListAggAggregationSingleton> Initialize(int groupingLength, IStateManagerClient stateManagerClient)
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
            var tree = await stateManagerClient.GetOrCreateTree("listaggtree", 
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
            {
                Comparer = new ListAggAggregationInsertComparer(groupingLength + 1),
                KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                ValueSerializer = new ValueListSerializer<int>(new IntSerializer())
            });

            return new ListAggAggregationSingleton(tree, groupingLength);
        }

        private static System.Linq.Expressions.Expression ListAggMapFunction(
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
                throw new InvalidOperationException("List_agg must have one argument.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            var expr = GetListAggBody();
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

        private static Expression<Func<FlxValue, byte[], long, ListAggAggregationSingleton, RowEvent, ValueTask<byte[]>>> GetListAggBody()
        {
            return (ev, bytes, weight, singleton, groupingKey) => DoListAgg(ev, bytes, weight, singleton, groupingKey);
        }

        private static async ValueTask<byte[]> DoListAgg(FlxValue column, byte[] currentState, long weight, ListAggAggregationSingleton singleton, RowEvent groupingKey)
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

        private static async ValueTask<FlxValue> ListAggGetValue(byte[]? state, RowEvent groupingKey, ListAggAggregationSingleton singleton)
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

            singleton.OutputBuilder.NewObject();
            int vectorStart = singleton.OutputBuilder.StartVector();

            bool stop = false;
            await foreach (var page in iterator)
            {
                if (stop)
                {
                    break;
                }
                foreach (var kv in page)
                {
                    if (singleton.AreKeyEqual(kv.Key, row))
                    {
                        singleton.OutputBuilder.Add(kv.Key.GetColumn(singleton.KeyLength));
                    }
                    else
                    {
                        stop = true;
                        break;
                    }
                }
            }
            singleton.OutputBuilder.EndVector(vectorStart, false, false);
            var bytes = singleton.OutputBuilder.Finish();
            return FlxValue.FromBytes(bytes);
        }

        private static async Task Commit(ListAggAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulAggregateFunction<ListAggAggregationSingleton>(
                FunctionsList.Uri,
                FunctionsList.ListAgg,
                Initialize,
                (singleton) => { },
                Commit,
                ListAggMapFunction,
                ListAggGetValue
                );
        }
    }
}
