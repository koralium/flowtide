﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class DoubleColumnTests
    {
        [Fact]
        public void TestAddAndGetByIndex()
        {
            var column = new DoubleColumn(GlobalMemoryManager.Instance);
            int i1 = column.Add(new DoubleValue(1));
            int i2 = column.Add(new DoubleValue(3));
            int i3 = column.Add(new DoubleValue(2));

            Assert.Equal(0, i1);
            Assert.Equal(1, i2);
            Assert.Equal(2, i3);

            Assert.Equal(1, column.GetValueAt(i1, default).AsDouble);
            Assert.Equal(3, column.GetValueAt(i2, default).AsDouble);
            Assert.Equal(2, column.GetValueAt(i3, default).AsDouble);
        }

        [Fact]
        public void TestSearchBoundries()
        {
            var column = new DoubleColumn(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(1));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(3));
            column.Add(new DoubleValue(4));
            column.Add(new DoubleValue(4));
            column.Add(new DoubleValue(5));
            column.Add(new DoubleValue(6));
            column.Add(new DoubleValue(7));

            var (start, end) = column.SearchBoundries(new DoubleValue(2), 0, 9, default, false);
            Assert.Equal(1, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new DoubleValue(3), 0, 9, default, false);
            Assert.Equal(5, start);
            Assert.Equal(5, end);

            (start, end) = column.SearchBoundries(new DoubleValue(4), 0, 9, default, false);
            Assert.Equal(6, start);
            Assert.Equal(7, end);

            (start, end) = column.SearchBoundries(new DoubleValue(9), 0, 9, default, false);
            Assert.Equal(~10, start);
            Assert.Equal(~10, end);

            var emptyColumn = new DoubleColumn(GlobalMemoryManager.Instance);
            (start, end) = emptyColumn.SearchBoundries(new DoubleValue(4), 0, -1, default, false);
            Assert.Equal(~0, start);
            Assert.Equal(~0, end);
        }

        [Fact]
        public void TestCompareTo()
        {
            var column = new DoubleColumn(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(0));
            column.Add(new DoubleValue(1));
            column.Add(new DoubleValue(2));
            Assert.Equal(-1, column.CompareTo(0, new DoubleValue(1), default, default));
            Assert.Equal(0, column.CompareTo(1, new DoubleValue(1), default, default));
            Assert.Equal(1, column.CompareTo(2, new DoubleValue(1), default, default));
        }

        [Fact]
        public void RemoveRangeNotNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<double?> expected = new List<double?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                column.Add(new DoubleValue(i));
                expected.Add(i);
            }

            column.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);

            for (int i = 0; i < 100; i++)
            {
                if (expected[i] != null)
                {
                    Assert.Equal(expected[i], column.GetValueAt(i, default).AsDouble);
                }
                else
                {
                    Assert.True(column.GetValueAt(i, default).IsNull);
                }
            }
        }

        [Fact]
        public void RemoveRangeWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<double?> expected = new List<double?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                if (r.Next(0, 2) == 0)
                {
                    column.Add(new DoubleValue(i));
                    expected.Add(i);
                }
                else
                {
                    column.Add(NullValue.Instance);
                    expected.Add(null);
                }
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);
            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());

            for (int i = 0; i < 900; i++)
            {
                if (expected[i] != null)
                {
                    Assert.Equal(expected[i], column.GetValueAt(i, default).AsDouble);
                }
                else
                {
                    Assert.True(column.GetValueAt(i, default).IsNull);
                }
            }
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new DoubleValue(1.23));

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("1.23", json);
        }

        [Fact]
        public void TestCopy()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            for (int i = 0; i < 1000; i++)
            {
                column.Add(new DoubleValue(i));
            }

            Column copy = column.Copy(GlobalMemoryManager.Instance);

            for (int i = 0; i < 1000; i++)
            {
                Assert.Equal(i, copy.GetValueAt(i, default).AsDouble);
            }
        }

        [Fact]
        public void TestAddToHash()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new DoubleValue(1.23)
            };

            var hash = new XxHash32();
            column.AddToHash(0, default, hash);
            var columnHash = hash.GetHashAndReset();

            column.GetValueAt(0, default).AddToHash(hash);
            var valueHash = hash.GetHashAndReset();

            Assert.Equal(columnHash, valueHash);
        }
    }
}
