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

        public record MultipleSumResult(string? companyId, int userkey, long sum1, long sum2);

        [Fact]
        public async Task SumTestMultipleBounds()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) as value,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS INT) as othersum
            FROM users
            ");

            await WaitForUpdate();

            var actual = GetActualRows();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var sum2 = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    Queue<double> values = new Queue<double>();
                    Queue<double> values2 = new Queue<double>();
                    List<MultipleSumResult> output = new List<MultipleSumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        while (values.Count > 4)
                        {
                            var dequeued = values.Dequeue();
                            sum -= dequeued;
                        }
                        while (values2.Count > 2)
                        {
                            var dequeued = values2.Dequeue();
                            sum2 -= dequeued;
                        }
                        values.Enqueue(orderedByKey[i].DoubleValue);
                        values2.Enqueue(orderedByKey[i].DoubleValue);
                        sum += orderedByKey[i].DoubleValue;
                        sum2 += orderedByKey[i].DoubleValue;
                        output.Add(new MultipleSumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (long)sum, (long)sum2));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SumTestDuplicateRowDeleteOne()
        {

            AddOrUpdateUser(new Entities.User()
            {
                UserKey = 1,
                CompanyId = "1",
                DoubleValue = 123
            });
            AddOrUpdateUser(new Entities.User()
            {
                UserKey = 2,
                CompanyId = "1",
                DoubleValue = 123
            });

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                1 as userkey,    
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY DoubleValue ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users
            ");
                
            await WaitForUpdate();

            var actual = GetActualRows();

            DeleteUser(Users.First());

            await WaitForUpdate();

            var act2 = GetActualRows();

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
                        output.Add(new SumResult(orderedByKey[i].CompanyId, 1, (long)sum));

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
                        for (; z < (i + 3); z++)
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

        [Fact]
        public async Task RowNumberWithCalculatedPartition()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CONCAT(CompanyId, 'a') ORDER BY UserKey)
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
        public async Task MultiplePartitionColumns()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId, UserKey ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => $"{x.CompanyId}-{x.UserKey}")
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

        /// <summary>
        /// Checks that if a partition is emptied, its delete output is still sent
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task SingleRowInPartitionThenDelete()
        {
            GenerateCompanies(1);
            GenerateUsers(1);

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            DeleteUser(Users[0]);

            await WaitForUpdate();

            var expected = new List<RowNumberResult>();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task DeleteAllEntries()
        {
            GenerateData(10_000);

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            while (Users.Count > 0)
            {
                DeleteUser(Users[0]);
            }

            await WaitForUpdate();

            var expected = new List<RowNumberResult>();

            AssertCurrentDataEqual(expected);
        }


        public record LeadResult(string? companyId, int userkey, long? value);

        [Fact]
        public async Task LeadWithPartitionOneArgument()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LEAD(UserKey) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<LeadResult> output = new List<LeadResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        long? val = null;
                        if (i < orderedByKey.Count - 1)
                        {
                            val = orderedByKey[i + 1].UserKey;
                        }
                        output.Add(new LeadResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, val));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LeadWithPartitionTwoArgumentsStep2()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LEAD(UserKey, 2) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<LeadResult> output = new List<LeadResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        long? val = null;
                        if (i < orderedByKey.Count - 2)
                        {
                            val = orderedByKey[i + 2].UserKey;
                        }
                        output.Add(new LeadResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, val));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LeadWithPartitionThreeArguments()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LEAD(UserKey, 1, 0) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<LeadResult> output = new List<LeadResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        long? val = 0;
                        if (i < orderedByKey.Count - 1)
                        {
                            val = orderedByKey[i + 1].UserKey;
                        }
                        output.Add(new LeadResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, val));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task FilterOnRowNumberOtherWindowInProjection()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (ORDER BY UserKey)
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) = 1
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => $"{x.CompanyId}")
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                })
                .Where(x => x.value == 1)
                .GroupBy(x => "1")
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.userkey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].companyId, orderedByKey[i].userkey, i + 1));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task FilterOnRowNumber()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                1 as val
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) = 1
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => $"{x.CompanyId}")
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                })
                .Where(x => x.value == 1).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task FilterOnRowNumberSameWindowInProjection()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) % 2 = 0
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => $"{x.CompanyId}")
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, i + 1));
                    }
                    return output;
                })
                .Where(x => x.value % 2 == 0)
                .GroupBy(x => $"{x.companyId}")
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.userkey).ToList();
                    List<RowNumberResult> output = new List<RowNumberResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new RowNumberResult(orderedByKey[i].companyId, orderedByKey[i].userkey, i + 1));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LagWithPartitionOneArgument()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAG(UserKey) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();
            var act = GetActualRows();
            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<LeadResult> output = new List<LeadResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        long? val = null;
                        if (i > 0)
                        {
                            val = orderedByKey[i - 1].UserKey;
                        }
                        output.Add(new LeadResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, val));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LagWithPartitionTwoArgumentsStep2()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAG(UserKey, 2) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<LeadResult> output = new List<LeadResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        long? val = null;
                        if (i > 1)
                        {
                            val = orderedByKey[i - 2].UserKey;
                        }
                        output.Add(new LeadResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, val));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LagWithPartitionThreeArguments()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAG(UserKey, 1, 0) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<LeadResult> output = new List<LeadResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        long? val = 0;
                        if (i > 0)
                        {
                            val = orderedByKey[i - 1].UserKey;
                        }
                        output.Add(new LeadResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, val));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        public record LastValueResult(string? companyId, int userkey, long? value);

        [Fact]
        public async Task LastValueBoundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as value
            FROM users
            ");

            await WaitForUpdate();

            var actual = GetActualRows();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        val = null;
                        for (int j = i - 4; j <= i; j++)
                        {
                            if (j >= 0)
                            {
                                if (users[j].Visits != null)
                                {
                                    val = users[j].Visits;
                                }   
                            }
                        }
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }
    }
}
