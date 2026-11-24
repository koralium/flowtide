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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class RowStoreRelationEquality
    {
        private static ReadRelation Read(string? store, params int[] stored)
        {
            return new ReadRelation()
            {
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "a", "b", "c" },
                },
                NamedTable = new Type.NamedTable
                {
                    Names = new List<string>() { "t" }
                },
                RowStore = store == null ? null : new RowStoreDefinition() { Name = store, StoredColumns = stored.ToList() }
            };
        }

        private static FetchRequiredColumnsRelation Fetch(int reference, string store, params int[] columns)
        {
            return new FetchRequiredColumnsRelation()
            {
                Input = Read("rows", 1, 2),
                Fetches = new List<RowStoreFetch>()
                {
                    new RowStoreFetch() { ReferenceColumn = reference, StoreName = store, Columns = columns.ToList() }
                }
            };
        }

        [Fact]
        public void AReadWithTheSameRowStoreIsEqual()
        {
            Assert.Equal(Read("rows", 1, 2), Read("rows", 1, 2));
            Assert.Equal(Read("rows", 1, 2).GetHashCode(), Read("rows", 1, 2).GetHashCode());
            Assert.True(Read("rows", 1, 2) == Read("rows", 1, 2));
        }

        [Fact]
        public void AReadWithAnotherRowStoreIsNotEqual()
        {
            Assert.NotEqual(Read("rows", 1, 2), Read(null));
            Assert.NotEqual(Read("rows", 1, 2), Read("other", 1, 2));
            Assert.NotEqual(Read("rows", 1, 2), Read("rows", 2, 1));
            Assert.NotEqual(Read("rows", 1, 2), Read("rows", 1));
            Assert.NotEqual(Read("rows", 1, 2).GetHashCode(), Read(null).GetHashCode());
        }

        [Fact]
        public void ACopiedReadKeepsItsRowStore()
        {
            var read = Read("rows", 1, 2);
            var copy = read.Copy();
            Assert.Equal(read, copy);
            copy.RowStore!.StoredColumns.Add(0);
            Assert.NotEqual(read, copy);
        }

        [Fact]
        public void AReadWithARowStoreSendsTheOtherColumnsAndAReference()
        {
            Assert.Equal(3, Read(null).OutputLength);
            Assert.Equal(2, Read("rows", 1, 2).OutputLength);
            var read = Read("rows", 0);
            read.Emit = new List<int>() { 2, 0 };
            Assert.Equal(2, read.OutputLength);
        }

        [Fact]
        public void ANormalizationWithAnotherRowStoreIsNotEqual()
        {
            NormalizationRelation Normalization(string? store)
            {
                return new NormalizationRelation()
                {
                    KeyIndex = new List<int>() { 0 },
                    Input = Read(null),
                    RowStore = store == null ? null : new RowStoreDefinition() { Name = store, StoredColumns = new List<int>() { 2 } }
                };
            }
            Assert.Equal(Normalization("rows"), Normalization("rows"));
            Assert.Equal(Normalization("rows").GetHashCode(), Normalization("rows").GetHashCode());
            Assert.NotEqual(Normalization("rows"), Normalization(null));
            Assert.NotEqual(Normalization("rows"), Normalization("other"));
            Assert.Equal(3, Normalization("rows").OutputLength);
            Assert.Equal(3, Normalization(null).OutputLength);
        }

        [Fact]
        public void FetchRequiredColumnsEquality()
        {
            Assert.Equal(Fetch(1, "rows", 0, 1), Fetch(1, "rows", 0, 1));
            Assert.Equal(Fetch(1, "rows", 0, 1).GetHashCode(), Fetch(1, "rows", 0, 1).GetHashCode());
            Assert.True(Fetch(1, "rows", 0, 1) == Fetch(1, "rows", 0, 1));
            Assert.True(Fetch(1, "rows", 0, 1) != Fetch(1, "rows", 1, 0));
            Assert.NotEqual(Fetch(1, "rows", 0, 1), Fetch(0, "rows", 0, 1));
            Assert.NotEqual(Fetch(1, "rows", 0, 1), Fetch(1, "other", 0, 1));
            Assert.False(Fetch(1, "rows", 0, 1).Equals(null));
            Assert.Equal(4, Fetch(1, "rows", 0, 1).OutputLength);
        }
    }
}
