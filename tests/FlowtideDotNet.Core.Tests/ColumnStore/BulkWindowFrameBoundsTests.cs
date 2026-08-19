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

using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class BulkWindowFrameBoundsTests
    {
        private static WindowFunction RowsBetween(WindowBound lower, WindowBound upper)
        {
            return new WindowFunction()
            {
                ExtensionUri = "test",
                ExtensionName = "sum",
                Arguments = new List<Expression>(),
                LowerBound = lower,
                UpperBound = upper
            };
        }

        /// <summary>
        /// An offset that large reaches past any partition, so it must saturate to the partition
        /// start sentinel. Left as a finite frame it overflows the frame size to a negative
        /// number, which builds a negative capacity ring and spins the eviction loop forever.
        /// </summary>
        [Fact]
        public void HugePrecedingOffsetSaturatesToUnboundedPreceding()
        {
            var bounds = BulkWindowFrameBounds.Parse(RowsBetween(
                new PreceedingRowWindowBound() { Offset = long.MaxValue },
                new CurrentRowWindowBound()));

            Assert.Equal(BulkWindowFrameKind.UnboundedPreceding, bounds.Kind);
            Assert.Equal(long.MinValue, bounds.From);
        }

        [Fact]
        public void HugeOffsetsOnBothEndsSaturateToWholePartition()
        {
            var bounds = BulkWindowFrameBounds.Parse(RowsBetween(
                new PreceedingRowWindowBound() { Offset = long.MaxValue - 1 },
                new FollowingRowWindowBound() { Offset = long.MaxValue - 1 }));

            Assert.Equal(BulkWindowFrameKind.WholePartition, bounds.Kind);
        }

        /// <summary>
        /// A finite lower bound with an unbounded upper must keep the upper sentinel, the
        /// suffix implementations rely on it rather than on a frame size.
        /// </summary>
        [Fact]
        public void HugeFollowingOffsetSaturatesToTheEndSentinel()
        {
            var bounds = BulkWindowFrameBounds.Parse(RowsBetween(
                new CurrentRowWindowBound(),
                new FollowingRowWindowBound() { Offset = long.MaxValue }));

            Assert.Equal(long.MaxValue, bounds.To);
            Assert.Equal(0, bounds.From);
        }

        [Fact]
        public void OrdinaryOffsetsStayBounded()
        {
            var bounds = BulkWindowFrameBounds.Parse(RowsBetween(
                new PreceedingRowWindowBound() { Offset = 2 },
                new CurrentRowWindowBound()));

            Assert.Equal(BulkWindowFrameKind.BoundedRows, bounds.Kind);
            Assert.Equal(-2, bounds.From);
            Assert.Equal(0, bounds.To);
        }
    }
}
