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

using FlowtideDotNet.Substrait.Exceptions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class WindowFunctionTests : FlowtideAcceptanceBase
    {
        public WindowFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        public record SumResult(string? companyId, int userkey, long value);

        [Fact]
        public async Task SumTestBoundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users
            ");

            await WaitForUpdate();

            var actual = GetActualRows();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    Queue<double> values = new Queue<double>();
                    List<SumResult> output = new List<SumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        while (values.Count > 4)
                        {
                            var dequeued = values.Dequeue();
                            sum -= dequeued;
                        }
                        values.Enqueue(orderedByKey[i].DoubleValue);
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (long)sum));
                        
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        public record SumOnlyResult(long value);

        [Fact]
        public async Task SumTestOnlyWindowFunction()
        {
            GenerateData(10_000);

            await StartStream(@"
            INSERT INTO output
            SELECT CAST(sum(u.doublevalue) OVER(PARTITION BY CompanyId ORDER BY UserKey) AS INT) FROM users u
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<SumOnlyResult> output = new List<SumOnlyResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new SumOnlyResult((long)sum));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SumTestUnboundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<SumResult> output = new List<SumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (long)sum));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SumTestUnbounded()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId) AS INT) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = (long)g.Sum(x => x.DoubleValue);
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<SumResult> output = new List<SumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, sum));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SumTestNoPartitionWithOrdering()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (ORDER BY UserKey) AS INT) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => "1")
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<SumResult> output = new List<SumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (long)sum));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SumTestBoundedStartToBoundedEnd()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND 2 FOLLOWING) AS INT) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    Queue<double> values = new Queue<double>();
                    List<SumResult> output = new List<SumResult>();
                    for (int i = 0, z = 0; i < orderedByKey.Count; i++)
                    {   
                        for (; z < (i+ 3); z++)
                        {
                            if (z < orderedByKey.Count)
                            {
                                values.Enqueue(orderedByKey[z].DoubleValue);
                                sum += orderedByKey[z].DoubleValue;
                            }
                            else
                            {
                                values.Enqueue(0);
                            }
                            
                        }
                        while (values.Count > 7)
                        {
                            var dequeued = values.Dequeue();
                            sum -= dequeued;
                        }
                        
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (long)sum));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        public record RowNumberResult(string? companyId, int userkey, long value);

        [Fact]
        public async Task RowNumberWithPartition()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task RowNumberWithoutPartition()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => "1")
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task RowNumberWithouOrderByThrowsException()
        {
            GenerateData();

            var result = await Assert.ThrowsAsync<SubstraitParseException>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT 
                        CompanyId,
                        UserKey,
                        ROW_NUMBER() OVER ()
                    FROM users
                    ");
            });

            Assert.Equal("'row_number' function must have an order by clause", result.Message);
        }

        [Fact]
        public async Task RowNumberWithPartitionAndCrash()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);

            await Crash();

            GenerateData();

            await WaitForUpdate();

            expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        public record RowNumberMultipleResult(string? companyId, int userkey, long value1, long value2);

        [Fact]
        public async Task MultipleWindowFunctions()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) as window1,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey DESC) as window2
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberMultipleResult> output = new List<RowNumberMultipleResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberMultipleResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1, orderedByKey.Count - i));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);

            await Crash();

            GenerateData();

            await WaitForUpdate();

            expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberMultipleResult> output = new List<RowNumberMultipleResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberMultipleResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1, orderedByKey.Count - i));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }
    }
}
