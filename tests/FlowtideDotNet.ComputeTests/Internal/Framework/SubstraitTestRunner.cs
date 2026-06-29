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

using FlowtideDotNet.ComputeTests.Internal.Parser.Tests;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Substrait.Expressions;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Reflection;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    /// <summary>
    /// A class just to get the MethodInfo of the TestMethod otherwise the test runner will not work
    /// </summary>
    internal class TmpClass
    {
        public static MethodInfo GetTestMethodInfo()
        {
            return typeof(TmpClass).GetMethod(nameof(TestMethod), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)!;
        }
        public static void TestMethod()
        {

        }
    }

    internal class SubstraitTestRunnerContext(
    SubstraitTest test,
    IMessageBus messageBus,
    string? skipReason,
    ExceptionAggregator aggregator,
    CancellationTokenSource cancellationTokenSource) :
        TestRunnerContext<SubstraitTest>(test, messageBus, skipReason, Xunit.Sdk.ExplicitOption.Off, aggregator, cancellationTokenSource, TmpClass.GetTestMethodInfo(), [])
    {
    }

    internal class SubstraitTestRunner
        : TestRunner<SubstraitTestRunnerContext, SubstraitTest>
    {
        public static SubstraitTestRunner Instance { get; } = new();

        public async ValueTask<RunSummary> Run(
        SubstraitTest test,
        IMessageBus messageBus,
        string? skipReason,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
        {
            await using var ctxt = new SubstraitTestRunnerContext(test, messageBus, skipReason, aggregator, cancellationTokenSource);
            await ctxt.InitializeAsync();

            return await Run(ctxt);
        }

        protected override bool IsTestClassCreatable(SubstraitTestRunnerContext ctxt) =>
        false;

        protected override bool IsTestClassDisposable(
            SubstraitTestRunnerContext ctxt,
            object testClassInstance) =>
                false;

        protected override ValueTask<(object? Instance, SynchronizationContext? SyncContext, ExecutionContext? ExecutionContext)> CreateTestClassInstance(SubstraitTestRunnerContext ctxt)
        {
            throw new NotSupportedException();
        }

        private ValueTask<TimeSpan> InvokeScalarTest(SubstraitTestRunnerContext ctxt, object? testClassInstance)
        {
            var parsedTest = ScalarTestParser.Parse(ctxt.Test.TestCase.Text!);

            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);

            var expressionList = new List<Expression>();
            for (int argIndex = 0; argIndex < parsedTest.Arguments.Count; argIndex++)
            {
                expressionList.Add(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = argIndex
                    }
                });
            }

            var extensionUri = ctxt.Test.TestCase.TestClass.IncludeList![0];
            if (extensionUri.StartsWith("/extensions"))
            {
                //Remove extensions
                extensionUri = extensionUri.Substring(11);
            }

            var compiledMethod = ColumnProjectCompiler.Compile(new ScalarFunction()
            {
                Arguments = expressionList,
                ExtensionUri = extensionUri,
                ExtensionName = parsedTest.FunctionName,
                Options = parsedTest.Options
            }, register);

            Column[] columns = new Column[parsedTest.Arguments.Count];
            for (int i = 0; i < parsedTest.Arguments.Count; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
                columns[i].Add(parsedTest.Arguments[i]);
            }
            Column resultColumn = Column.Create(GlobalMemoryManager.Instance);
            // Run once to try and reduce IL compile time for output
            if (parsedTest.Expected.ExpectError)
            {
                Assert.ThrowsAny<Exception>(() => compiledMethod(new EventBatchData(columns), 0, resultColumn));
            }
            else
            {
                compiledMethod(new EventBatchData(columns), 0, resultColumn);
            }
            resultColumn.Clear();
            Stopwatch sw = new();
            sw.Start();
            if (parsedTest.Expected.ExpectError)
            {
                Assert.ThrowsAny<Exception>(() => compiledMethod(new EventBatchData(columns), 0, resultColumn));
            }
            else
            {
                compiledMethod(new EventBatchData(columns), 0, resultColumn);
            }
            sw.Stop();

            if (!parsedTest.Expected.ExpectError)
            {
                var actual = resultColumn.GetValueAt(0, default);
                Assert.Equal(parsedTest.Expected.ExpectedValue, actual, (x, y) => DataValueComparer.CompareTo(x!, y!) == 0);
            }
            return new(sw.Elapsed);
        }

        private async ValueTask<TimeSpan> InvokeAggregateTest(SubstraitTestRunnerContext ctxt, object? testClassInstance)
        {
            var parsedTest = AggregateTestParser.Parse(ctxt.Test.TestCase.Text!);

            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);

            var extensionUri = ctxt.Test.TestCase.TestClass.IncludeList![0];
            if (extensionUri.StartsWith("/extensions"))
            {
                //Remove extensions
                extensionUri = extensionUri.Substring(11);
            }

            var expressionList = new List<Expression>();
            for (int argIndex = 0; argIndex < parsedTest.InputData.Columns.Count; argIndex++)
            {
                expressionList.Add(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = argIndex
                    }
                });
            }

            AggregateFunction aggregateFunction = new AggregateFunction()
            {
                Arguments = expressionList,
                ExtensionName = parsedTest.FunctionName,
                ExtensionUri = extensionUri,
                Options = parsedTest.Options
            };

            using StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions(), NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter(""), "", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var stateClient = stateManager.GetOrCreateClient("a");

            if (register.TryGetBulkAggregationFunction(extensionUri, parsedTest.FunctionName, out var bulkFunc))
            {
                var measure = bulkFunc.Create(aggregateFunction, register);

                SharedGroupValueTree? sharedTree = null;
                if (measure is ISharedTreeColumnAggregation sharedMeasure && sharedMeasure.SupportsSharedTree)
                {
                    var sharedTreeName = "sharedtree";
                    var bTree = await stateClient.GetOrCreateTree(sharedTreeName,
                        new BPlusTreeOptions<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>()
                        {
                            Comparer = new BulkMinInsertComparer(0),
                            KeySerializer = new BulkGroupValueKeyStorageSerializer(0, GlobalMemoryManager.Instance),
                            ValueSerializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance),
                            UseByteBasedPageSizes = true,
                            MemoryAllocator = GlobalMemoryManager.Instance,
                            UsePreviousPointers = true
                        });
                    sharedTree = new SharedGroupValueTree("sharedtree", sharedMeasure.ValueProjection, bTree, bTree.CreateBulkInserter(), null, sharedMeasure.IgnoreNulls);
                    sharedTree.BindMeasure(sharedMeasure);
                    sharedMeasure.BindSharedTree(sharedTree.Tree, 0);
                    sharedMeasure.SetGroupMapping(new int[parsedTest.InputData.Count]);
                }

                await measure.InitializeAsync(0, stateClient, GlobalMemoryManager.Instance);

                using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                for (int i = 0; i < parsedTest.InputData.Count; i++)
                {
                    weights.Add(1);
                }

                Column stateColumn = Column.Create(GlobalMemoryManager.Instance);
                stateColumn.Add(NullValue.Instance);
                var stateColumnRef = new ColumnReference(stateColumn, 0, default);

                Stopwatch sw = new();
                sw.Start();

                measure.NewBatch(weights, parsedTest.InputData);
                if (sharedTree != null)
                {
                    sharedTree.NewBatch(weights, parsedTest.InputData, GlobalMemoryManager.Instance);
                }

                var sortedByGroupIndices = new int[parsedTest.InputData.Count];
                for (int i = 0; i < parsedTest.InputData.Count; i++)
                {
                    sortedByGroupIndices[i] = i;
                }

                if (parsedTest.InputData.Count > 0)
                {
                    if (sharedTree != null)
                    {
                        await sharedTree.StoreAsync(weights, new IColumn[0], sortedByGroupIndices, parsedTest.InputData);
                    }
                    else
                    {
                        await measure.StoreAsync(weights, new IColumn[0], parsedTest.InputData, sortedByGroupIndices);
                    }
                }
                measure.Compute(sortedByGroupIndices, weights, parsedTest.InputData, stateColumnRef, 0);

                if (sharedTree != null)
                {
                    await sharedTree.Tree.Commit();
                }
                await measure.CommitAsync();
                sw.Stop();

                Column outputColumn = Column.Create(GlobalMemoryManager.Instance);
                await measure.FetchValuesAsync(new IColumn[0], 1, outputColumn);
                await measure.GetValuesAsync(new IColumn[0], new ColumnReference[] { stateColumnRef }, 0, 1, outputColumn);

                var actual = outputColumn.GetValueAt(0, default);
                Assert.Equal(parsedTest.ExpectedResult.ExpectedValue, actual, (x, y) => DataValueComparer.CompareTo(x!, y!) == 0);

                return sw.Elapsed;
            }
            else
            {
                var compileResult = await ColumnMeasureCompiler.CompileMeasure(0, stateClient, aggregateFunction, register, GlobalMemoryManager.Instance);

                IColumn[] groupingBatchColumns = new IColumn[0];
                var groupBatch = new EventBatchData(groupingBatchColumns);
                var groupingKey = new ColumnRowReference() { referenceBatch = groupBatch, RowIndex = 0 };

                Column stateColumn = Column.Create(GlobalMemoryManager.Instance);
                stateColumn.Add(NullValue.Instance);
                var stateColumnRef = new ColumnReference(stateColumn, 0, default);

                Column outputColumn = Column.Create(GlobalMemoryManager.Instance);

                Stopwatch sw = new();
                sw.Start();

                for (int i = 0; i < parsedTest.InputData.Count; i++)
                {
                    await compileResult.Compute(groupingKey, parsedTest.InputData, i, stateColumnRef, 1);
                }
                sw.Stop();

                await compileResult.GetValue(groupingKey, stateColumnRef, outputColumn);

                var actual = outputColumn.GetValueAt(0, default);

                Assert.Equal(parsedTest.ExpectedResult.ExpectedValue, actual, (x, y) => DataValueComparer.CompareTo(x!, y!) == 0);

                return sw.Elapsed;
            }
        }

        protected override ValueTask<TimeSpan> InvokeTest(SubstraitTestRunnerContext ctxt, object? testClassInstance)
        {
            if (ctxt.Test.TestCase.TestClass.IsScalar)
            {
                return InvokeScalarTest(ctxt, testClassInstance);
            }
            else
            {
                return InvokeAggregateTest(ctxt, testClassInstance);
            }
        }
    }
}
