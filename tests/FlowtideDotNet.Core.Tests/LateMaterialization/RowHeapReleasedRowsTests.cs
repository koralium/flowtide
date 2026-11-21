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

using FlowtideDotNet.Core.Operators.LateMaterialization;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowHeapReleasedRowsTests
    {
        private const long Sequence = 7;

        // Compares every slot and stored word, also after a restore
        private static void AssertKnown(RowHeapReleasedRows released, bool[] gone)
        {
            int rows = gone.Length;
            Assert.True(released.TryGet(Sequence, out var view));
            Assert.Equal(rows, view.Rows);
            var words = new ulong[(rows + 63) >> 6];
            for (int slot = 0; slot < rows; slot++)
            {
                if (gone[slot])
                {
                    words[slot >> 6] |= 1UL << (slot & 63);
                }
                Assert.True(view.IsReleased(slot) == gone[slot], $"Slot {slot} of {rows} is told wrong.");
            }
            int last = 0;
            for (int word = 0; word < words.Length; word++)
            {
                Assert.Equal(words[word], view.Word(word));
                last = words[word] != 0 ? word : last;
            }
            var restored = new RowHeapReleasedRows();
            restored.Restore(Sequence, rows, words.AsSpan(0, last + 1));
            Assert.True(restored.TryGet(Sequence, out var again));
            Assert.Equal(rows, again.Rows);
            for (int slot = 0; slot < rows; slot++)
            {
                Assert.True(again.IsReleased(slot) == gone[slot], $"Slot {slot} of {rows} is told wrong after a restore.");
            }
            Assert.True(restored.ByteSize <= released.ByteSize, $"A restore keeps {restored.ByteSize} bytes for what took {released.ByteSize}.");
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(63)]
        [InlineData(64)]
        [InlineData(65)]
        [InlineData(120)]
        [InlineData(121)]
        [InlineData(500)]
        [InlineData(8000)]
        [InlineData(65536)]
        public void EveryReleasedRowIsKnownUntilTheLastOneGoes(int rows)
        {
            var released = new RowHeapReleasedRows();
            var order = Enumerable.Range(0, rows).ToArray();
            new Random(rows).Shuffle(order);
            var gone = new bool[rows];
            int live = rows;
            int checkEvery = Math.Max(1, rows / 40);

            for (int i = 0; i < rows - 1; i++)
            {
                var outcome = released.Release(Sequence, order[i], live, out int known);
                Assert.Equal(RowHeapReleaseOutcome.Released, outcome);
                Assert.Equal(rows, known);
                gone[order[i]] = true;
                live--;
                if (i % checkEvery == 0 || i == rows - 2)
                {
                    AssertKnown(released, gone);
                    Assert.Equal(RowHeapReleaseOutcome.AlreadyReleased, released.Release(Sequence, order[i], live, out _));
                    Assert.Equal(RowHeapReleaseOutcome.BeyondRows, released.Release(Sequence, rows, live, out _));
                }
            }
            var lastRow = released.Release(Sequence, order[rows - 1], 1, out _);
            bool keptUntilForgotten = released.TryGet(Sequence, out var view) && !view.IsReleased(order[rows - 1]);
            released.Forget(Sequence);

            Assert.Equal(RowHeapReleaseOutcome.Released, lastRow);
            Assert.True(keptUntilForgotten);
            Assert.False(released.TryGet(Sequence, out _));
            Assert.Equal(0, released.Count);
            Assert.Equal(0, released.ByteSize);
        }

        [Fact]
        public void ASequenceWithoutReleasedRowsHasAsManyRowsAsLiveOnes()
        {
            var released = new RowHeapReleasedRows();

            var beyond = released.Release(Sequence, 5, 5, out int rows);
            var onlyRow = released.Release(Sequence + 1, 0, 1, out int oneRow);
            var first = released.Release(Sequence, 4, 5, out _);

            Assert.Equal(RowHeapReleaseOutcome.BeyondRows, beyond);
            Assert.Equal(5, rows);
            Assert.Equal(RowHeapReleaseOutcome.Released, onlyRow);
            Assert.Equal(1, oneRow);
            Assert.Equal(RowHeapReleaseOutcome.Released, first);
            Assert.False(released.TryGet(Sequence + 1, out _));
            Assert.Equal(1, released.Count);
        }

        [Fact]
        public void SequencesOfUpTo120RowsHaveTheirBitsInTheTable()
        {
            var released = new RowHeapReleasedRows();
            for (long sequence = 1; sequence <= 100; sequence++)
            {
                released.Release(sequence, 3, 120, out _);
                released.Release(sequence, 119, 119, out _);
            }
            long inTable = released.ByteSize;
            released.Release(500, 3, 121, out _);

            Assert.Equal(100 * RowHeapReleasedRows.InlineEntryBytes, inTable);
            Assert.Equal(inTable + RowHeapReleasedRows.LargeEntryBytes + RowHeapReleasedRows.ArrayOverhead + 4 * sizeof(ushort), released.ByteSize);
            Assert.Equal(101, released.Count);
        }

        [Fact]
        public void ALargeSequenceKeepsAListWhileThatIsSmallerThanItsBits()
        {
            const int rows = 8000;
            const int entry = RowHeapReleasedRows.LargeEntryBytes + RowHeapReleasedRows.ArrayOverhead;
            var released = new RowHeapReleasedRows();
            var sizes = new Dictionary<int, long>();
            int live = rows;
            for (int slot = 0; slot < rows - 1; slot++)
            {
                released.Release(Sequence, slot, live, out _);
                live--;
                sizes[rows - live] = released.ByteSize;
            }

            Assert.Equal(entry + 4 * sizeof(ushort), sizes[1]);
            Assert.Equal(entry + 8 * sizeof(ushort), sizes[5]);
            Assert.Equal(entry + 512 * sizeof(ushort), sizes[500]);
            Assert.Equal(entry + 125 * sizeof(ulong), sizes[501]);
            Assert.Equal(entry + 125 * sizeof(ulong), sizes[7499]);
            Assert.Equal(entry + 500 * sizeof(ushort), sizes[7500]);
            Assert.Equal(entry + 250 * sizeof(ushort), sizes[7750]);
            Assert.Equal(entry + 4 * sizeof(ushort), sizes[7999]);
        }
    }
}
