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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Hash
{
    public class BatchHasherTests
    {
        [Fact]
        public void SingleKeyBatchHashingMatchesExpected()
        {
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new Int64Value(10));
            col.Add(new Int64Value(20));
            col.Add(NullValue.Instance);
            col.Add(new Int64Value(30));

            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new int[] { 0 });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(4, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 4; i++)
            {
                expectedHasher.Reset();
                col.AddToHash(i, default, expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void MultiKeyBatchHashingMatchesExpected()
        {
            var col1 = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col1.Add(new Int64Value(1));
            col1.Add(new Int64Value(2));
            col1.Add(new Int64Value(3));

            var col2 = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col2.Add(new StringValue("alpha"));
            col2.Add(new StringValue("beta"));
            col2.Add(new StringValue("gamma"));

            var batch = new EventBatchData(new IColumn[] { col1, col2 });
            var hasher = new BatchHasher(new int[] { 0, 1 });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(3, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col1.AddToHash(i, default, expectedHasher);
                col2.AddToHash(i, default, expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void MultiKeyBatchHashingWithNullsMatchesExpected()
        {
            var col1 = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col1.Add(new Int64Value(42));
            col1.Add(NullValue.Instance);
            col1.Add(new Int64Value(99));
            col1.Add(NullValue.Instance);

            var col2 = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col2.Add(NullValue.Instance);
            col2.Add(new StringValue("test"));
            col2.Add(new StringValue("flowtide"));
            col2.Add(NullValue.Instance);

            var batch = new EventBatchData(new IColumn[] { col1, col2 });
            var hasher = new BatchHasher(new int[] { 0, 1 });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(4, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 4; i++)
            {
                expectedHasher.Reset();
                col1.AddToHash(i, default, expectedHasher);
                col2.AddToHash(i, default, expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void MultiKeyBatchHashingWithFieldReferences()
        {
            var col1 = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col1.Add(new Int64Value(100));
            col1.Add(new Int64Value(200));

            var col2 = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col2.Add(new StringValue("foo"));
            col2.Add(new StringValue("bar"));

            var batch = new EventBatchData(new IColumn[] { col1, col2 });
            var fieldRefs = new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0 } },
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 1 } }
            };
            var hasher = new BatchHasher(fieldRefs);

            var actual = hasher.HashBatch(batch);
            Assert.Equal(2, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 2; i++)
            {
                expectedHasher.Reset();
                col1.AddToHash(i, default, expectedHasher);
                col2.AddToHash(i, default, expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void SingleKeyOnlyEmptyStrings()
        {
            // Only empty strings leaves string memory unallocated
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new StringValue(""));
            col.Add(NullValue.Instance);
            col.Add(new StringValue(""));

            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new int[] { 0 });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(3, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col.AddToHash(i, default, expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void SingleKeyStructFieldMissingInVariantHashesAsNull()
        {
            // Different headers become a union of struct columns
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new StructValue(StructHeader.Create("a"), new Int64Value(1)));
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(2)));
            col.Add(NullValue.Instance);

            var child = new StructReferenceSegment { Field = 1 };
            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = child } }
            });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(3, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col.GetValueAt(i, child).AddToHash(expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void MultiKeyStructFieldMissingInVariantHashesAsNull()
        {
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new StructValue(StructHeader.Create("a"), new Int64Value(1)));
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(2)));
            col.Add(NullValue.Instance);

            var missingChild = new StructReferenceSegment { Field = 1 };
            var existingChild = new StructReferenceSegment { Field = 0 };
            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = missingChild } },
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = existingChild } }
            });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(3, actual.Length);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col.GetValueAt(i, missingChild).AddToHash(expectedHasher);
                col.GetValueAt(i, existingChild).AddToHash(expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void SingleKeyStructFieldByNameHashesTheField()
        {
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(2)));
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(3)));
            col.Add(NullValue.Instance);

            var child = new MapKeyReferenceSegment { Key = "a" };
            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = child } }
            });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(3, actual.Length);
            Assert.Equal(actual[0], actual[1]);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col.GetValueAt(i, child).AddToHash(expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void MultiKeyStructFieldByNameHashesTheField()
        {
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(2)));
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(3)));
            col.Add(NullValue.Instance);

            var nameChild = new MapKeyReferenceSegment { Key = "a" };
            var missingChild = new MapKeyReferenceSegment { Key = "c" };
            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = nameChild } },
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = missingChild } }
            });

            var actual = hasher.HashBatch(batch);
            Assert.Equal(3, actual.Length);
            Assert.Equal(actual[0], actual[1]);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col.GetValueAt(i, nameChild).AddToHash(expectedHasher);
                col.GetValueAt(i, missingChild).AddToHash(expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), actual[i]);
            }
        }

        [Fact]
        public void StructFieldByNameInVariantsHashesTheField()
        {
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            col.Add(new StructValue(StructHeader.Create("a", "b"), new Int64Value(1), new Int64Value(2)));
            col.Add(new StructValue(StructHeader.Create("b", "a"), new Int64Value(3), new Int64Value(1)));
            col.Add(NullValue.Instance);

            var child = new MapKeyReferenceSegment { Key = "a" };
            var batch = new EventBatchData(new IColumn[] { col });
            var singleKeyHasher = new BatchHasher(new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = child } }
            });
            var multiKeyHasher = new BatchHasher(new List<FieldReference>
            {
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = child } },
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0, Child = child } }
            });

            var singleKey = singleKeyHasher.HashBatch(batch).ToArray();
            var multiKey = multiKeyHasher.HashBatch(batch).ToArray();
            Assert.Equal(singleKey[0], singleKey[1]);
            Assert.Equal(multiKey[0], multiKey[1]);

            var expectedHasher = new XxHash32();
            for (int i = 0; i < 3; i++)
            {
                expectedHasher.Reset();
                col.GetValueAt(i, child).AddToHash(expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), singleKey[i]);
                col.GetValueAt(i, child).AddToHash(expectedHasher);
                Assert.Equal(expectedHasher.GetCurrentHashAsUInt32(), multiKey[i]);
            }
        }

        [Fact]
        public void EmptyBatchReturnsEmptySpan()
        {
            var col = ColumnFactory.Get(GlobalMemoryManager.Instance);
            var batch = new EventBatchData(new IColumn[] { col });
            var hasher = new BatchHasher(new int[] { 0 });

            var actual = hasher.HashBatch(batch);
            Assert.True(actual.IsEmpty);
        }

        [Fact]
        public void EmptyFieldsThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => new BatchHasher(Array.Empty<int>()));
            Assert.Throws<ArgumentException>(() => new BatchHasher(Array.Empty<FieldReference>()));
        }
    }
}
