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

using FlowtideDotNet.Benchmarks.ReservoirReaders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;
using System.Globalization;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// The identity hash against the engine's XxHash32, run with --row-hash.
    /// </summary>
    internal static class RowIdentityHashBench
    {
        private const int Rows = 1000;

        public static int Run(string[] args)
        {
            string arm = ReservoirReaderBench.Option(args, "arm", "kernels");
            string shape = ReservoirReaderBench.Option(args, "shape", "mixed");
            int rounds = int.Parse(ReservoirReaderBench.Option(args, "rounds", "3000"), CultureInfo.InvariantCulture);
            if (arm != "kernels" && arm != "values" && arm != "xxhash32")
            {
                Console.Error.WriteLine($"Unknown arm '{arm}', use kernels, values or xxhash32.");
                return 2;
            }
            var columns = Columns(shape);
            if (columns.Count == 0)
            {
                Console.Error.WriteLine($"Unknown shape '{shape}', use int, string, decimal, ints, nulls or mixed.");
                return 2;
            }
            var batch = new EventBatchData(columns.ToArray());
            var picked = new int[columns.Count];
            for (int i = 0; i < picked.Length; i++)
            {
                picked[i] = i;
            }
            var hasher = new BatchHasher(picked);
            var hashes = new ulong[Rows];
            ulong sum = 0;
            double best = double.MaxValue;
            // The first pass warms up and is not counted
            for (int pass = 0; pass < 7; pass++)
            {
                var watch = Stopwatch.StartNew();
                for (int round = 0; round < rounds; round++)
                {
                    if (arm == "xxhash32")
                    {
                        sum += hasher.HashBatch(batch)[round % Rows];
                    }
                    else if (arm == "values")
                    {
                        hashes.AsSpan().Fill(RowIdentityHash.Seed);
                        for (int c = 0; c < columns.Count; c++)
                        {
                            RowIdentityHash.AddValues(columns[c], 0, hashes);
                        }
                        sum += hashes[round % Rows];
                    }
                    else
                    {
                        RowIdentityHash.HashRows(columns, picked, 0, hashes);
                        sum += hashes[round % Rows];
                    }
                }
                double nanos = watch.Elapsed.TotalNanoseconds / (rounds * (double)Rows);
                if (pass > 0 && nanos < best)
                {
                    best = nanos;
                }
            }
            Console.WriteLine(FormattableString.Invariant($"{arm} {shape} ({columns.Count} columns): {best:F1} ns per row (sum {sum})"));
            return 0;
        }

        private static List<IColumn> Columns(string shape)
        {
            var random = new Random(3);
            var columns = new List<IColumn>();
            switch (shape)
            {
                case "int":
                    columns.Add(Column(i => new Int64Value(random.NextInt64())));
                    break;
                case "string":
                    columns.Add(Column(i => new StringValue("customer name " + random.Next(1_000_000))));
                    break;
                case "decimal":
                    columns.Add(Column(i => new DecimalValue(new decimal(random.Next(1, 10_000_000), 0, 0, false, 2))));
                    break;
                case "ints":
                    for (int c = 0; c < 6; c++)
                    {
                        columns.Add(Column(i => new Int64Value(random.Next(100_000))));
                    }
                    break;
                case "nulls":
                    columns.Add(Column(i => i % 4 == 0 ? NullValue.Instance : new Int64Value(random.Next(100_000))));
                    columns.Add(Column(i => i % 7 == 0 ? NullValue.Instance : new StringValue("customer name " + random.Next(1_000_000))));
                    break;
                case "mixed":
                    columns.Add(Column(i => new Int64Value(random.NextInt64())));
                    columns.Add(Column(i => new StringValue("customer name " + random.Next(1_000_000))));
                    columns.Add(Column(i => new DecimalValue(new decimal(random.Next(1, 10_000_000), 0, 0, false, 2))));
                    columns.Add(Column(i => new DoubleValue(random.NextDouble())));
                    columns.Add(Column(i => new StringValue(new string('x', 80) + random.Next(1_000_000))));
                    columns.Add(Column(i => i % 10 == 0 ? NullValue.Instance : new Int64Value(i)));
                    break;
            }
            return columns;
        }

        private static Column Column(Func<int, IDataValue> make)
        {
            var column = Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < Rows; i++)
            {
                column.Add(make(i));
            }
            return column;
        }
    }
}
