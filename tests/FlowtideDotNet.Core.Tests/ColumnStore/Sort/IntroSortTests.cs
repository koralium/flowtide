using FlowtideDotNet.Core.ColumnStore.Sort;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Sort
{
    public class IntroSortTests
    {
        private struct IntComparer : IComparer<int>
        {
            public int Compare(int x, int y) => x.CompareTo(y);
        }

        [Fact]
        public void TestSortEmptySpan()
        {
            int[] array = Array.Empty<int>();
            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);
            Assert.Empty(array);
        }

        [Fact]
        public void TestSortSingleElement()
        {
            int[] array = new[] { 1 };
            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);
            Assert.Equal(new[] { 1 }, array);
        }

        [Fact]
        public void TestSortTwoElementsSorted()
        {
            int[] array = new[] { 1, 2 };
            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);
            Assert.Equal(new[] { 1, 2 }, array);
        }

        [Fact]
        public void TestSortTwoElementsUnsorted()
        {
            int[] array = new[] { 2, 1 };
            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);
            Assert.Equal(new[] { 1, 2 }, array);
        }

        [Fact]
        public void TestSortThreeElements()
        {
            var permutations = new[]
            {
                new[] { 1, 2, 3 },
                new[] { 1, 3, 2 },
                new[] { 2, 1, 3 },
                new[] { 2, 3, 1 },
                new[] { 3, 1, 2 },
                new[] { 3, 2, 1 }
            };

            foreach (var array in permutations)
            {
                var comparer = new IntComparer();
                IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);
                Assert.Equal(new[] { 1, 2, 3 }, array);
            }
        }

        [Fact]
        public void TestSortInsertionSortThreshold()
        {
            var rnd = new Random(123);
            var array = Enumerable.Range(0, 16).OrderBy(x => rnd.Next()).ToArray();
            var expected = array.OrderBy(x => x).ToArray();
            
            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);
            
            Assert.Equal(expected, array);
        }

        [Fact]
        public void TestSortQuickSort()
        {
            var rnd = new Random(1234);
            var array = Enumerable.Range(0, 100).OrderBy(x => rnd.Next()).ToArray();
            var expected = array.OrderBy(x => x).ToArray();

            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);

            Assert.Equal(expected, array);
        }

        [Fact]
        public void TestSortAllIdentical()
        {
            int[] array = Enumerable.Repeat(5, 50).ToArray();
            var expected = array.ToArray();

            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);

            Assert.Equal(expected, array);
        }

        [Fact]
        public void TestSortReversed()
        {
            int[] array = Enumerable.Range(0, 1000).Reverse().ToArray();
            var expected = array.OrderBy(x => x).ToArray();

            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);

            Assert.Equal(expected, array);
        }

        [Fact]
        public void TestSortLargeRandom()
        {
            int n = 100000;
            var array = new int[n];
            var rnd = new Random(456);
            for (int i = 0; i < n; i++)
            {
                array[i] = rnd.Next();
            }
            var expected = array.ToArray();
            Array.Sort(expected);

            var comparer = new IntComparer();
            IntroSort.Sort<int, IntComparer>(array.AsSpan(), ref comparer);

            Assert.Equal(expected, array);
        }
    }
}
