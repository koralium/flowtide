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

using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    public class LineageMergeTests
    {
        private static readonly LineageTransformation DirectIdentity = new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity);
        private static readonly LineageTransformation DirectTransformation = new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation);
        private static readonly LineageTransformation DirectAggregation = new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation);
        private static readonly LineageTransformation IndirectFilter = new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Filter);
        private static readonly LineageTransformation IndirectJoin = new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join);
        private static readonly LineageTransformation IndirectGroupBy = new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy);

        [Fact]
        public void MergeSingleReturnsSameInstance()
        {
            var lineage = Lineage(("c1", [Field("in1", "a", DirectIdentity)]));

            Assert.Same(lineage, LineageMerge.Merge([lineage]));
        }

        [Fact]
        public void MergeUnionsColumnsInFirstSeenOrder()
        {
            var first = Lineage(("c1", [Field("in1", "a", DirectIdentity)]), ("c2", [Field("in1", "b", DirectIdentity)]));
            var second = Lineage(("c3", [Field("in2", "c", DirectIdentity)]), ("c1", [Field("in2", "a", DirectIdentity)]));

            var merged = LineageMerge.Merge([first, second]);

            Assert.Equal(["c1", "c2", "c3"], merged.Fields.Keys);
        }

        [Fact]
        public void MergeUnionsInputFieldsPerColumn()
        {
            var first = Lineage(("c1", [Field("in1", "a", DirectIdentity)]));
            var second = Lineage(("c1", [Field("in2", "a", DirectIdentity), Field("in1", "a", DirectIdentity)]));

            var merged = LineageMerge.Merge([first, second]);

            Assert.Equal([Field("in1", "a", DirectIdentity), Field("in2", "a", DirectIdentity)], merged.Fields["c1"].InputFields);
        }

        [Fact]
        public void MergeTransformationsDropsIdentity()
        {
            Assert.Equal([DirectTransformation], LineageMerge.MergeTransformations([DirectIdentity], [DirectTransformation]));
            Assert.Equal([DirectAggregation], LineageMerge.MergeTransformations([DirectAggregation], [DirectIdentity]));
        }

        [Fact]
        public void MergeTransformationsKeepsDirectAndIndirect()
        {
            Assert.Equal([DirectIdentity, IndirectFilter], LineageMerge.MergeTransformations([DirectIdentity], [IndirectFilter]));
        }

        [Fact]
        public void MergeTransformationsIsDistinct()
        {
            Assert.Equal([DirectIdentity], LineageMerge.MergeTransformations([DirectIdentity], [DirectIdentity]));
        }

        [Fact]
        public void MergeInputFieldsMergesTransformationsOnCollision()
        {
            var merged = LineageMerge.MergeInputFields([Field("in1", "a", DirectIdentity)], [Field("in1", "a", DirectTransformation)]);

            Assert.Equal([Field("in1", "a", DirectTransformation)], merged);
        }

        [Fact]
        public void MergeDatasetUnion()
        {
            var first = Lineage([Field("in1", "x", IndirectJoin)], ("c1", []));
            var second = Lineage([Field("in1", "x", IndirectFilter), Field("in1", "y", IndirectGroupBy)], ("c1", []));

            var merged = LineageMerge.Merge([first, second]);

            Assert.Equal([
                new LineageInputField("ns", "in1", "x", [IndirectJoin, IndirectFilter]),
                Field("in1", "y", IndirectGroupBy)
                ], merged.Dataset);
        }

        [Fact]
        public void MergeColumnsUpgradesAnyType()
        {
            var merged = LineageMerge.MergeColumns([
                [new LineageColumn("c", AnyType.Instance), new LineageColumn("d", new StringType())],
                [new LineageColumn("c", new Int64Type()), new LineageColumn("d", new Int64Type()), new LineageColumn("e", new BoolType())]
                ]);

            Assert.Equal([
                new LineageColumn("c", new Int64Type()),
                new LineageColumn("d", new StringType()),
                new LineageColumn("e", new BoolType())
                ], merged);
        }

        [Fact]
        public void ToColumnsFillsMissingTypesWithAny()
        {
            Assert.Null(LineageMerge.ToColumns(null));

            var columns = LineageMerge.ToColumns(new NamedStruct()
            {
                Names = ["a", "b"],
                Struct = new Struct() { Types = [new Int64Type()] }
            });

            Assert.Equal([new LineageColumn("a", new Int64Type()), new LineageColumn("b", AnyType.Instance)], columns);
        }

        private static LineageInputField Field(string table, string field, LineageTransformation transformation)
        {
            return new LineageInputField("ns", table, field, [transformation]);
        }

        private static ColumnLineage Lineage(params (string Name, LineageInputField[] Inputs)[] fields)
        {
            return Lineage([], fields);
        }

        private static ColumnLineage Lineage(LineageInputField[] dataset, params (string Name, LineageInputField[] Inputs)[] fields)
        {
            var dictionary = new Dictionary<string, ColumnLineageField>();
            foreach (var (name, inputs) in fields)
            {
                dictionary.Add(name, new ColumnLineageField(inputs));
            }
            return new ColumnLineage(dictionary, dataset);
        }
    }
}
