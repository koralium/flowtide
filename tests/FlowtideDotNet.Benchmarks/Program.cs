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

using BenchmarkDotNet.Running;
using DifferntialCompute.Benchmarks;
using FlowtideDotNet.Benchmarks;

//BatchSortBenchmark batchSortBenchmark = new BatchSortBenchmark();
//batchSortBenchmark.GlobalSetup();


//for (int i = 0; i < 10000; i++)
//{
//    batchSortBenchmark.BeforeIteration();
//    batchSortBenchmark.SortMethod();
//}


//BPlusTreeBulkSearchBenchmark bPlusTreeBulkSearchBenchmark = new BPlusTreeBulkSearchBenchmark();
//bPlusTreeBulkSearchBenchmark.TreeSize = 1_000_000;
//bPlusTreeBulkSearchBenchmark.SearchKeyCount = 1000;
//bPlusTreeBulkSearchBenchmark.GlobalSetup();
//await bPlusTreeBulkSearchBenchmark.BulkSearchPreSorted();

//ColumnStoreTreeBenchmark columnStore = new ColumnStoreTreeBenchmark();
//columnStore.GlobalSetup();
//columnStore.IterationSetup();
//await columnStore.ColumnarInsertBulkInOrder();
//BPlusTreeBulkInsertBenchmark bPlusTreeBulkInsertBenchmark = new BPlusTreeBulkInsertBenchmark();
//bPlusTreeBulkInsertBenchmark.ElementCount = 1_000_000;
//bPlusTreeBulkInsertBenchmark.GlobalSetup();
//bPlusTreeBulkInsertBenchmark.IterationSetup();
//await bPlusTreeBulkInsertBenchmark.BulkInsert_Batched();

//Console.WriteLine("hello");
var summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);