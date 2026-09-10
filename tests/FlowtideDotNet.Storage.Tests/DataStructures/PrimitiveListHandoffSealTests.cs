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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Storage.Tests.DataStructures
{
    public class PrimitiveListHandoffSealTests
    {
        private static PrimitiveList<int> SealedList()
        {
            var list = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 4; i++)
            {
                list.Add(i);
            }
            list.SealForHandoff();
            return list;
        }

        private static void Mutate(PrimitiveList<int> list, string mutator)
        {
            using var other = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            other.Add(9);
            switch (mutator)
            {
                case "EnsureCapacity":
                    list.EnsureCapacity(1024);
                    break;
                case "Add":
                    list.Add(5);
                    break;
                case "AddRangeFrom":
                    list.AddRangeFrom(other, 0, 1);
                    break;
                case "InsertAt":
                    list.InsertAt(0, 5);
                    break;
                case "InsertRangeFrom":
                    list.InsertRangeFrom(0, other, 0, 1);
                    break;
                case "InsertStaticRange":
                    list.InsertStaticRange(0, 5, 2);
                    break;
                case "InsertFromList":
                    ReadOnlySpan<int> lookup = [0];
                    ReadOnlySpan<int> positions = [0];
                    list.InsertFrom(in other, in lookup, in positions, -1);
                    break;
                case "InsertFromArray":
                    list.InsertFrom([9], [0], [0]);
                    break;
                case "DeleteBatch":
                    list.DeleteBatch([0]);
                    break;
                case "MoveAtIndex":
                    list.MoveAtIndex(0, 1);
                    break;
                case "RemoveAt":
                    list.RemoveAt(0);
                    break;
                case "RemoveRange":
                    list.RemoveRange(0, 1);
                    break;
                case "Update":
                    list.Update(0, 5);
                    break;
                case "Indexer":
                    list[0] = 5;
                    break;
                case "Clear":
                    list.Clear();
                    break;
                case "SetLength":
                    list.SetLength(1);
                    break;
                default:
                    throw new ArgumentException(mutator);
            }
        }

        [Theory]
        [InlineData("EnsureCapacity")]
        [InlineData("Add")]
        [InlineData("AddRangeFrom")]
        [InlineData("InsertAt")]
        [InlineData("InsertRangeFrom")]
        [InlineData("InsertStaticRange")]
        [InlineData("InsertFromList")]
        [InlineData("InsertFromArray")]
        [InlineData("DeleteBatch")]
        [InlineData("MoveAtIndex")]
        [InlineData("RemoveAt")]
        [InlineData("RemoveRange")]
        [InlineData("Update")]
        [InlineData("Indexer")]
        [InlineData("Clear")]
        [InlineData("SetLength")]
        public void EveryMutatorThrowsWhenSealed(string mutator)
        {
            using var list = SealedList();

            var ex = Assert.Throws<InvalidOperationException>(() => Mutate(list, mutator));

            Assert.Contains("already handed downstream", ex.Message);
            Assert.Equal(4, list.Count);
        }

        [Fact]
        public void ReadsStillWorkWhenSealed()
        {
            using var list = SealedList();

            Assert.Equal(2, list.Get(2));
            Assert.Equal(3, list[3]);
            Assert.Equal(4, list.Span.Length);
        }

        [Fact]
        public void UnsealedListIsUnaffected()
        {
            using var list = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            list.Add(1);
            list[0] = 2;
            list.RemoveAt(0);

            Assert.Equal(0, list.Count);
        }
    }
}
