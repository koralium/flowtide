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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.Statement;

namespace FlowtideDotNet.Core.Compute.Internal.StatefulAggregations
{
    internal class StringAggAggregationSingleton
    {
        private readonly int keyLength;

        public StringAggAggregationSingleton(IBPlusTree<RowEvent, int> tree, int keyLength)
        {
            Tree = tree;
            this.keyLength = keyLength;
        }
        public int KeyLength => keyLength;
        public IBPlusTree<RowEvent, int> Tree { get; }
        public string? Seperator { get; set; }

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
    internal class StringAggAggregation
    {
        private static async ValueTask<FlxValue> StringAggGetValue(byte[]? state, RowEvent groupingKey, StringAggAggregationSingleton singleton)
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

            StringBuilder sb = new StringBuilder();

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
                        // Add duplicate values
                        for (int i = 0; i < kv.Value; i++)
                        {
                            sb.Append(kv.Key.GetColumn(singleton.KeyLength).AsString).Append(singleton.Seperator);
                        }
                    }
                    else
                    {
                        stop = true;
                        break;
                    }
                }
            }

            if (sb.Length > 0)
            {
                sb.Remove(sb.Length - singleton.Seperator!.Length, singleton.Seperator.Length);
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(sb.ToString()));
        }

        private static async Task<StringAggAggregationSingleton> Initialize(int groupingLength, IStateManagerClient stateManagerClient)
        {
            var tree = await stateManagerClient.GetOrCreateTree("stringaggtree", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int>()
            {
                Comparer = new ListAggAggregationInsertComparer(groupingLength + 1),
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new IntSerializer()
            });

            return new StringAggAggregationSingleton(tree, groupingLength);
        }

        private static async Task Commit(StringAggAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        private static Expression<Func<FlxValue, string, byte[], long, StringAggAggregationSingleton, RowEvent, ValueTask<byte[]>>> GetStringAggBody()
        {
            return (ev, seperator, bytes, weight, singleton, groupingKey) => DoStringAgg(ev, seperator, bytes, weight, singleton, groupingKey);
        }

        private static async ValueTask<byte[]> DoStringAgg(FlxValue column, string seperator, byte[] currentState, long weight, StringAggAggregationSingleton singleton, RowEvent groupingKey)
        {
            if (column.IsNull)
            {
                return currentState;
            }
            if (singleton.Seperator == null)
            {
                singleton.Seperator = seperator;
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

        private static System.Linq.Expressions.Expression StringAggMapFunction(
           Substrait.Expressions.AggregateFunction function,
           ParametersInfo parametersInfo,
           ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
           ParameterExpression stateParameter,
           ParameterExpression weightParameter,
           ParameterExpression singletonAccess,
           ParameterExpression groupingKeyParameter)
        {
            if (function.Arguments.Count != 2)
            {
                throw new InvalidOperationException("string_agg must have two arguments.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            var arg2 = visitor.Visit(function.Arguments[1], parametersInfo);

            if (arg2 is not ConstantExpression constantExpression)
            {
                throw new InvalidOperationException("Seperator must be a constant string.");
            }
            if (constantExpression.Value is not FlxValue flxValue ||
                flxValue.ValueType != FlexBuffers.Type.String)
            {
                throw new InvalidOperationException("Seperator must be a constant string.");
            }
            var seperator = flxValue.AsString;
            var expr = GetStringAggBody();
            var body = expr.Body;
            var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
            System.Linq.Expressions.Expression e = replacer.Visit(body);
            replacer = new ParameterReplacerVisitor(expr.Parameters[1], System.Linq.Expressions.Expression.Constant(seperator));
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[2], stateParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[3], weightParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[4], singletonAccess);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[5], groupingKeyParameter);
            e = replacer.Visit(e);
            return e;
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulAggregateFunction<StringAggAggregationSingleton>(
                FunctionsString.Uri,
                FunctionsString.StringAgg,
                Initialize,
                (singleton) => { },
                Commit,
                StringAggMapFunction,
                StringAggGetValue
                );
        }
    }
}
