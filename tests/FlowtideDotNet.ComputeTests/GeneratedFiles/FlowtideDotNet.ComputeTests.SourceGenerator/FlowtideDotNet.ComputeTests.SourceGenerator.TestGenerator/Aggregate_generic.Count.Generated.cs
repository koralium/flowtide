using System;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;

namespace Aggregate_generic
{
    public class Count
    {
        public static IEnumerable<object[]> GetDataForTest0()
        {
            yield return new object[] { "count", new EventBatchData([new Column(GlobalMemoryManager.Instance) { new Int64Value(100), new Int64Value(-200), new Int64Value(300), new Int64Value(-400), new Int64Value(5), new Int64Value(6) }]), new Int64Value(6) };
            yield return new object[] { "count", new EventBatchData([new Column(GlobalMemoryManager.Instance) { new Int64Value(1000) }]), new Int64Value(1) };
            yield return new object[] { "count", new EventBatchData([new Column(GlobalMemoryManager.Instance) {  }]), new Int64Value(0) };
            yield return new object[] { "count", new EventBatchData([new Column(GlobalMemoryManager.Instance) { NullValue.Instance, NullValue.Instance, NullValue.Instance }]), new Int64Value(0) };
            yield return new object[] { "count", new EventBatchData([new Column(GlobalMemoryManager.Instance) { NullValue.Instance, NullValue.Instance, NullValue.Instance, new Int64Value(1000) }]), new Int64Value(1) };
        }

        [Theory(DisplayName = "basic: Basic examples without any special cases")]
        [MemberData(nameof(GetDataForTest0))]
        public async Task Test0(string functionName, EventBatchData rowBatch, IDataValue expected)
        {
            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);
            var expressionList = new List<Expression>();
            for (int argIndex = 0; argIndex < rowBatch.Columns.Count; argIndex++)
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
                ExtensionName = functionName,
                ExtensionUri = "/functions_aggregate_generic.yaml",
                Arguments = expressionList
            };
            StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions(), NullLogger.Instance, new System.Diagnostics.Metrics.Meter(""), "");
            var stateClient = stateManager.GetOrCreateClient("a");
            var compileResult = await ColumnMeasureCompiler.CompileMeasure(0, stateClient, aggregateFunction, register, GlobalMemoryManager.Instance);
            IColumn[] groupingBatchColumns = new IColumn[0];
            var groupBatch = new EventBatchData(groupingBatchColumns);
            var groupingKey = new ColumnRowReference() { referenceBatch = groupBatch, RowIndex = 0 };
            Column stateColumn = Column.Create(GlobalMemoryManager.Instance);
            stateColumn.Add(NullValue.Instance);
            var stateColumnRef = new ColumnReference(stateColumn, 0, default);
            for (int i = 0; i < rowBatch.Count; i++)
            {
                await compileResult.Compute(groupingKey, rowBatch, i, stateColumnRef, 1);
            }
            Column outputColumn = Column.Create(GlobalMemoryManager.Instance);
            await compileResult.GetValue(groupingKey, stateColumnRef, outputColumn);
            var actual = outputColumn.GetValueAt(0, default);
            Assert.Equal(expected, actual, (x, y) => DataValueComparer.CompareTo(x, y) == 0);
        }
    }
}
