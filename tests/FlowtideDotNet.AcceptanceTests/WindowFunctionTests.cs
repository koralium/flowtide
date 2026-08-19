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

using FlowtideDotNet.AcceptanceTests.Entities;
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

        public record AverageResult(string? companyId, int userkey, double value);

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
            ", ignoreSameDataCheck: true);

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
        public async Task FilterOnRowNumberAlias()
        {
            GenerateData();

            // Top N pattern, the where clause reads the row number through its select list alias.
            // The projected value must be the one that was filtered on, not a recomputed one.
            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) as rownum
            FROM users
            WHERE rownum <= 3
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
                .Where(x => x.value <= 3).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task FilterOnRowNumberAliasFromSubQuery()
        {
            GenerateData();

            // The nexmark q6 shape, the row number alias is filtered inside the sub query
            await StartStream(@"
            INSERT INTO output
            SELECT
                q.CompanyId,
                q.UserKey,
                q.rownum
            FROM (
                SELECT
                    CompanyId,
                    UserKey,
                    ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) as rownum
                FROM users
                WHERE rownum <= 1
            ) q
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => $"{x.CompanyId}")
                .Select(g =>
                {
                    var first = g.OrderBy(x => x.UserKey).First();
                    return new RowNumberResult(first.CompanyId, first.UserKey, 1);
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task WildcardDoesNotIncludeWindowFunctionColumn()
        {
            GenerateData();

            // '*' must expand to the columns of the input only, the window function
            // gets a column of its own that the wildcard must not pick up
            await StartStream(@"
            INSERT INTO output
            SELECT *, ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) as rownum
            FROM (
                SELECT
                    CompanyId,
                    UserKey
                FROM users
            ) u
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
        public async Task LastValueIgnoreNullBoundedStartToCurrentRow()
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

        [Fact]
        public async Task LastValueIgnoreNullBoundedStartToBoundedEnd()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND 2 FOLLOWING) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        val = null;
                        for (int j = i - 4; j <= (i + 2) && j < users.Count; j++)
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

        [Fact]
        public async Task LastValueIgnoreNullUnbounded()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        if (users[i].Visits != null)
                        {
                            val = users[i].Visits;
                        }
                    }
                    for (int i = 0; i < users.Count; i++)
                    {
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LastValueIgnoreNullUnboundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        val = null;
                        for (int j = 0; j <= i; j++)
                        {
                            if (users[j].Visits != null)
                            {
                                val = users[j].Visits;
                            }
                        }
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LastValueIgnoreNullDefaultBound()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY userkey) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        val = null;
                        for (int j = 0; j <= i; j++)
                        {
                            if (users[j].Visits != null)
                            {
                                val = users[j].Visits;
                            }
                        }
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LastValueRespectNullBoundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as value
            FROM users
            ");

            await WaitForUpdate();

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
                                val = users[j].Visits;
                            }
                        }
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LastValueRespectNullUnbounded()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        val = users[i].Visits;
                    }
                    for (int i = 0; i < users.Count; i++)
                    {
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LastValueRespectNullUnboundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    var users = x.OrderBy(x => x.UserKey).ToList();

                    long? val = default;
                    List<LastValueResult> output = new List<LastValueResult>();
                    for (int i = 0; i < users.Count; i++)
                    {
                        val = null;
                        for (int j = 0; j <= i; j++)
                        {
                            val = users[j].Visits;
                        }
                        output.Add(new LastValueResult(users[i].CompanyId, users[i].UserKey, val));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LastValueNoOrderByError()
        {
            GenerateData();

            var ex = await Assert.ThrowsAsync<SubstraitParseException>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT 
                        CompanyId,
                        UserKey,
                        LAST_VALUE(Visits) OVER (PARTITION BY CompanyId) as value
                    FROM users
                    ");
            });

            Assert.Equal("'last_value' function must have an order by clause", ex.Message);
        }

        private record MinByResult(string? companyId, int orderkey, int userkey, long value);

        [Fact]
        public async Task MinByNoNullsBoundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                u.CompanyId,
                o.OrderKey,
                o.UserKey,
                min_by(o.UserKey, o.UserKey) OVER (PARTITION BY u.CompanyId ORDER BY OrderKey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as value
            FROM orders o
            INNER JOIN users u
            ON o.userkey = u.userkey
            ");

            await WaitForUpdate();

            var expected = Orders.Join(Users, x => x.UserKey, x => x.UserKey, (order, user) => new { user, order}).GroupBy(x => x.user.CompanyId)
                .SelectMany(x =>
                {
                    var sorted = x.OrderBy(x => x.order.OrderKey).ToList();
                    var values = new Queue<long>();
                    var min = long.MaxValue;
                    List<MinByResult> output = new List<MinByResult>();

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        values.Enqueue(sorted[i].order.UserKey);
                        min = Math.Min(min, sorted[i].order.UserKey);
                        while (values.Count > 5)
                        {
                            var dequeued = values.Dequeue();
                            if (dequeued == min)
                            {
                                min = long.MaxValue;
                                foreach (var v in values)
                                {
                                    min = Math.Min(min, v);
                                }
                            }
                        }
                        output.Add(new MinByResult(sorted[i].user.CompanyId, sorted[i].order.OrderKey, sorted[i].order.UserKey, min));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MinByNoNullsUnboundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                u.CompanyId,
                o.OrderKey,
                o.UserKey,
                min_by(o.UserKey, o.UserKey) OVER (PARTITION BY u.CompanyId ORDER BY OrderKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM orders o
            INNER JOIN users u
            ON o.userkey = u.userkey
            ");

            await WaitForUpdate();

            var expected = Orders.Join(Users, x => x.UserKey, x => x.UserKey, (order, user) => new { user, order }).GroupBy(x => x.user.CompanyId)
                .SelectMany(x =>
                {
                    var sorted = x.OrderBy(x => x.order.OrderKey).ToList();
                    var min = long.MaxValue;
                    List<MinByResult> output = new List<MinByResult>();

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        min = Math.Min(min, sorted[i].order.UserKey);
                        output.Add(new MinByResult(sorted[i].user.CompanyId, sorted[i].order.OrderKey, sorted[i].order.UserKey, min));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MinByNoNullsUnboundedStartToUnboundedEnd()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                u.CompanyId,
                o.OrderKey,
                o.UserKey,
                min_by(o.UserKey, o.UserKey) OVER (PARTITION BY u.CompanyId ORDER BY OrderKey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM orders o
            INNER JOIN users u
            ON o.userkey = u.userkey
            ");

            await WaitForUpdate();

            var expected = Orders.Join(Users, x => x.UserKey, x => x.UserKey, (order, user) => new { user, order }).GroupBy(x => x.user.CompanyId)
                .SelectMany(x =>
                {
                    var sorted = x.OrderBy(x => x.order.OrderKey).ToList();
                    var min = long.MaxValue;
                    List<MinByResult> output = new List<MinByResult>();

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        min = Math.Min(min, sorted[i].order.UserKey);
                    }

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        output.Add(new MinByResult(sorted[i].user.CompanyId, sorted[i].order.OrderKey, sorted[i].order.UserKey, min));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MaxByNoNullsBoundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                u.CompanyId,
                o.OrderKey,
                o.UserKey,
                max_by(o.UserKey, o.UserKey) OVER (PARTITION BY u.CompanyId ORDER BY OrderKey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as value
            FROM orders o
            INNER JOIN users u
            ON o.userkey = u.userkey
            ");

            await WaitForUpdate();

            var expected = Orders.Join(Users, x => x.UserKey, x => x.UserKey, (order, user) => new { user, order }).GroupBy(x => x.user.CompanyId)
                .SelectMany(x =>
                {
                    var sorted = x.OrderBy(x => x.order.OrderKey).ToList();
                    var values = new Queue<long>();
                    var max = long.MinValue;
                    List<MinByResult> output = new List<MinByResult>();

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        values.Enqueue(sorted[i].order.UserKey);
                        max = Math.Max(max, sorted[i].order.UserKey);
                        while (values.Count > 5)
                        {
                            var dequeued = values.Dequeue();
                            if (dequeued == max)
                            {
                                max = long.MinValue;
                                foreach (var v in values)
                                {
                                    max = Math.Max(max, v);
                                }
                            }
                        }
                        output.Add(new MinByResult(sorted[i].user.CompanyId, sorted[i].order.OrderKey, sorted[i].order.UserKey, max));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MaxByNoNullsUnboundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                u.CompanyId,
                o.OrderKey,
                o.UserKey,
                max_by(o.UserKey, o.UserKey) OVER (PARTITION BY u.CompanyId ORDER BY OrderKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM orders o
            INNER JOIN users u
            ON o.userkey = u.userkey
            ");

            await WaitForUpdate();

            var expected = Orders.Join(Users, x => x.UserKey, x => x.UserKey, (order, user) => new { user, order }).GroupBy(x => x.user.CompanyId)
                .SelectMany(x =>
                {
                    var sorted = x.OrderBy(x => x.order.OrderKey).ToList();
                    var max = long.MinValue;
                    List<MinByResult> output = new List<MinByResult>();

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        max = Math.Max(max, sorted[i].order.UserKey);
                        output.Add(new MinByResult(sorted[i].user.CompanyId, sorted[i].order.OrderKey, sorted[i].order.UserKey, max));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MaxByNoNullsUnboundedStartToUnboundedEnd()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                u.CompanyId,
                o.OrderKey,
                o.UserKey,
                max_by(o.UserKey, o.UserKey) OVER (PARTITION BY u.CompanyId ORDER BY OrderKey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM orders o
            INNER JOIN users u
            ON o.userkey = u.userkey
            ");

            await WaitForUpdate();

            var expected = Orders.Join(Users, x => x.UserKey, x => x.UserKey, (order, user) => new { user, order }).GroupBy(x => x.user.CompanyId)
                .SelectMany(x =>
                {
                    var sorted = x.OrderBy(x => x.order.OrderKey).ToList();
                    var max = long.MinValue ;
                    List<MinByResult> output = new List<MinByResult>();

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        max = Math.Max(max, sorted[i].order.UserKey);
                    }

                    for (int i = 0; i < sorted.Count; i++)
                    {
                        output.Add(new MinByResult(sorted[i].user.CompanyId, sorted[i].order.OrderKey, sorted[i].order.UserKey, max));
                    }

                    return output;
                });

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MinByInIterationLoop()
        {
            GenerateGraphNodes(10);

            await StartStream(@"
            WITH cte AS (
            SELECT
                Id,
                parentId,
                min_by(g.Id, g.Id) OVER (PARTITION BY ParentId ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reducedId,
                1 as level
            FROM graphnodes g
            UNION ALL
            SELECT
               g.Id,
               c.reducedId as parentId,
               min_by(g.Id, g.Id) OVER (PARTITION BY c.reducedId ORDER BY g.Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reducedId,
               c.level + 1 as level
            FROM graphnodes g
            INNER JOIN cte c ON g.parentId = c.reducedId
            )
            INSERT INTO output
            SELECT 
                Id,
                reducedId,
                parentId
            FROM cte
            WHERE ROW_NUMBER() OVER (PARTITION BY Id ORDER BY level DESC) = 1;
            ");

            await WaitForUpdate();

            ValidateGraphData();

            // Add more nodes
            GenerateGraphNodes(10);

            await WaitForUpdate();

            ValidateGraphData();

            // Delete the first node that is dependent on the root, replace its children to be dependent on the root
            var firstNode = GraphNodes.First();
            var firstDependentOnFirst = GraphNodes.First(x => x.ParentId == firstNode.Id);
            var dependents = GraphNodes.Where(x => x.ParentId == firstDependentOnFirst.Id).ToList();

            foreach(var d in dependents)
            {
                d.ParentId = firstNode.Id;
                AddOrUpdateGraphNode(d);
            }
            DeleteGraphNode(firstDependentOnFirst);

            await WaitForUpdate();

            ValidateGraphData();
        }

        private void ValidateGraphData()
        {
            List<ExpectedGraphNode> expected = new List<ExpectedGraphNode>();
            foreach (var e in GraphNodes)
            {
                expected.Add(new ExpectedGraphNode()
                {
                    id = e.Id,
                    parentId = e.ParentId,
                    reducedId = e.Id,
                    level = 1
                });
            }

            var firstNode = expected.First();
            CreateExpectedGraphNodes(firstNode.id!.Value, firstNode, expected, 2);

            AssertCurrentDataEqual(expected.Select(x => new { x.id, x.reducedId, x.parentId }));
        }

        private class ExpectedGraphNode
        {
            public int? id;
            public int? reducedId;
            public int? parentId;
            public int level;

            public override string ToString()
            {
                return $"{{ id = {id}, reducedId = {reducedId}, parentId = {parentId}, level = {level} }}";
            }
        }

        private void CreateExpectedGraphNodes(int lookupId, ExpectedGraphNode node, List<ExpectedGraphNode> nodes, int level)
        {
            var dependentNodes = nodes.Where(x => x.parentId == lookupId).ToList();

            var firstNode = dependentNodes.OrderBy(x => x.id).FirstOrDefault();
            foreach (var dependentNode in dependentNodes)
            {
                var currentId = dependentNode.id;
                dependentNode.reducedId = firstNode!.id;
                dependentNode.parentId = node.id;
                dependentNode.level = level;
                CreateExpectedGraphNodes(currentId!.Value, dependentNode, nodes, level + 1);
            }
        }

        [Fact]
        public async Task AverageTestUnboundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<AverageResult> output = new List<AverageResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new AverageResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, ((double)sum / (i + 1))));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task AverageTestBoundedStartToCurrentRow()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) as value
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
                    int counter = 0;
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        while (values.Count > 4)
                        {
                            var dequeued = values.Dequeue();
                            sum -= dequeued;
                            counter--;
                        }
                        values.Enqueue(orderedByKey[i].DoubleValue);
                        sum += orderedByKey[i].DoubleValue;
                        counter++;
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (int)((double)sum / counter)));

                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task AverageTestUnbounded()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT 
                CompanyId,
                UserKey,
                CAST(AVG(DoubleValue) OVER (PARTITION BY CompanyId) AS INT) as value
            FROM users
            ");

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var avg = (long)g.Average(x => x.DoubleValue);
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    List<SumResult> output = new List<SumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        output.Add(new SumResult(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, avg));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        public record MixedAverageResult(string? companyId, int userkey, double value);

        /// <summary>
        /// A non numeric value in the frame counts like a null, it must not change the divisor.
        /// The seeded rescan refills the previous frame, so the row after it is the one at risk.
        /// </summary>
        [Fact]
        public async Task BoundedAverageIgnoresNonNumericValueInASeededFrame()
        {
            AddOrUpdateUser(new User() { UserKey = 10, CompanyId = "1", DoubleValue = 10, FirstName = "str" });
            AddOrUpdateUser(new User() { UserKey = 20, CompanyId = "1", DoubleValue = 20, FirstName = "str" });
            AddOrUpdateUser(new User() { UserKey = 30, CompanyId = "1", DoubleValue = 30, FirstName = "str" });

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                AVG(CASE WHEN UserKey = 20 THEN FirstName ELSE DoubleValue END)
                    OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new MixedAverageResult("1", 10, 10),
                new MixedAverageResult("1", 20, 10),
                new MixedAverageResult("1", 30, 30)
            });

            // Lands directly after the non numeric row, so its rescan seeds a frame holding it
            AddOrUpdateUser(new User() { UserKey = 25, CompanyId = "1", DoubleValue = 25, FirstName = "str" });
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new MixedAverageResult("1", 10, 10),
                new MixedAverageResult("1", 20, 10),
                new MixedAverageResult("1", 25, 25),
                new MixedAverageResult("1", 30, 27.5)
            });
        }

        public record LagOffsetResult(string? companyId, int userkey, long? value);

        public record LagStringResult(string? companyId, int userkey, string? firstName, string? lagValue);

        public record LastValueDoubleResult(string? companyId, int userkey, double? value);

        public record LastValueLongResult(string? companyId, int userkey, long? value);

        public record LastValueAndMinByResult(string? companyId, int userkey, double? lastValue, double? minValue);

        public record RespectAndIgnoreResult(string? companyId, int userkey, long? respectValue, long? ignoreValue);

        public record JoinResult(string? companyId, int userkey, decimal? money);

        public record StabilityRow(string? firstName, double? windowValue);

        private void AddUser(int userKey)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = "1"
            });
        }

        private void AddUser(string companyId, int userKey, double doubleValue)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                DoubleValue = doubleValue
            });
        }

        private void AddUserVisits(string companyId, int userKey, int? visits)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                Visits = visits
            });
        }

        private void AddNamedUser(int userKey, string firstName)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = "1",
                FirstName = firstName
            });
        }

        private void AddNamedUser(int userKey, double doubleValue, string firstName)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = "1",
                FirstName = firstName,
                DoubleValue = doubleValue
            });
        }

        private void AddLagOffsetPartition()
        {
            for (int i = 1; i <= 5; i++)
            {
                AddUser(i * 10);
            }
        }

        private List<LagOffsetResult> ExpectedAllNull()
        {
            return Users.OrderBy(x => x.UserKey)
                .Select(x => new LagOffsetResult(x.CompanyId, x.UserKey, null))
                .ToList();
        }

        // Bounds the wait so a stalled stream fails instead of hanging
        private async Task WaitForUpdateBounded(string message)
        {
            var update = WaitForUpdate();
            var finished = await Task.WhenAny(update, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(finished == update, message);
            await update;
        }

        // A lag offset larger than the partition is null, not a fault
        [Fact]
        public async Task LagWithMaxLongOffsetReturnsNullInsteadOfCrashing()
        {
            AddLagOffsetPartition();

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAG(UserKey, 9223372036854775807) OVER (PARTITION BY CompanyId ORDER BY UserKey) as value
            FROM users");
            await WaitForUpdateBounded("The stream never produced a result for a lag offset of long.MaxValue");

            AssertCurrentDataEqual(ExpectedAllNull());
        }

        // Offsets above int range stay null, including after a mid partition insert
        [Fact]
        public async Task LagWithOffsetAboveIntMaxReturnsNull()
        {
            AddLagOffsetPartition();

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAG(UserKey, 3000000000) OVER (PARTITION BY CompanyId ORDER BY UserKey) as value
            FROM users");
            await WaitForUpdateBounded("The stream never produced a result for a lag offset of 3000000000");

            AssertCurrentDataEqual(ExpectedAllNull());

            AddUser(25);
            await WaitForUpdateBounded("The stream never produced a result after the incremental insert");

            AssertCurrentDataEqual(ExpectedAllNull());
        }

        private List<LagStringResult> ExpectedLag(int offset)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<LagStringResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        var lag = i - offset >= 0 ? ordered[i - offset].FirstName : null;
                        output.Add(new LagStringResult(ordered[i].CompanyId, ordered[i].UserKey, ordered[i].FirstName, lag));
                    }
                    return output;
                }).ToList();
        }

        private const string LagQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                FirstName,
                LAG(FirstName) OVER (PARTITION BY CompanyId ORDER BY UserKey) as lagValue
            FROM users";

        // Varying name lengths so a wrong value shows up as the wrong string
        private void AddVaryingLengthNames()
        {
            AddNamedUser(10, "a");
            AddNamedUser(20, "bb");
            AddNamedUser(30, "cccc");
            AddNamedUser(40, "d");
            AddNamedUser(50, "eeeeeee");
            AddNamedUser(60, "ff");
        }

        // Lag over a string column returns the previous row's value
        [Fact]
        public async Task LagOverStringColumnEmitsPreviousValue()
        {
            AddVaryingLengthNames();

            await StartStream(LagQuery);
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedLag(1));
        }

        // Lag with offset two over a string column returns the value two rows back
        [Fact]
        public async Task LagWithOffsetTwoOverStringColumnEmitsValueTwoBack()
        {
            AddVaryingLengthNames();

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                FirstName,
                LAG(FirstName, 2) OVER (PARTITION BY CompanyId ORDER BY UserKey) as lagValue
            FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedLag(2));
        }

        // String lag stays correct after an insert in the middle of the partition
        [Fact]
        public async Task LagOverStringColumnStaysCorrectAfterIncrementalInsert()
        {
            AddVaryingLengthNames();

            await StartStream(LagQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLag(1));

            AddNamedUser(35, new string('z', 12));
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedLag(1));
        }

        // A single row following frame is null once it runs past the partition end
        [Fact]
        public async Task LastValueRespectNullsFollowingOnlyFrameIsNullPastPartitionEnd()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                LAST_VALUE(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            var expected = Enumerable.Range(0, 10)
                .Select(i => new LastValueDoubleResult("1", i, i + 4 <= 9 ? (double?)(i + 4) : null));
            AssertCurrentDataEqual(expected);
        }

        // A frame that starts after it ends is empty, so the value is null
        [Fact]
        public async Task LastValueRespectNullsEmptyFrameFromGreaterThanToIsNull()
        {
            for (int i = 0; i < 6; i++)
            {
                AddUserVisits("1", i, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) as value
            FROM users");
            await WaitForUpdate();

            var expected = Users.Select(x => new LastValueLongResult(x.CompanyId, x.UserKey, null)).ToList();
            AssertCurrentDataEqual(expected);
        }

        // Two functions over the same frame see the same rows
        [Fact]
        public async Task LastValueRespectNullsAndMinByAgreeOnEmptyFrame()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                LAST_VALUE(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) as lastValue,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) as minValue
            FROM users");
            await WaitForUpdate();

            var expected = Enumerable.Range(0, 10)
                .Select(i => new LastValueAndMinByResult(
                    "1",
                    i,
                    i + 4 <= 9 ? (double?)(i + 4) : null,
                    i + 4 <= 9 ? (double?)(i + 4) : null));
            AssertCurrentDataEqual(expected);
        }

        // A suffix frame starting past the partition end is empty
        [Fact]
        public async Task LastValueRespectNullsFollowingToUnboundedIsNullPastPartitionEnd()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                LAST_VALUE(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            var expected = Enumerable.Range(0, 10)
                .Select(i => new LastValueDoubleResult("1", i, i + 4 <= 9 ? (double?)9 : null));
            AssertCurrentDataEqual(expected);
        }

        // Null treatment cannot change the answer over an empty frame
        [Fact]
        public async Task LastValueRespectAndIgnoreNullsAgreeOnEmptyFrame()
        {
            for (int i = 0; i < 6; i++)
            {
                AddUserVisits("1", i, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) as respectValue,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) as ignoreValue
            FROM users");
            await WaitForUpdate();

            var expected = Users.Select(x => new RespectAndIgnoreResult(x.CompanyId, x.UserKey, null, null)).ToList();
            AssertCurrentDataEqual(expected);
        }

        private const int NullOrderingRowCount = 200;

        // A small source batch makes the join deliver many batches
        private void SeedMatchedUsersAndOrders()
        {
            SourceBatchSize = 16;
            for (int i = 1; i <= NullOrderingRowCount; i++)
            {
                AddOrUpdateUser(new User() { UserKey = i, CompanyId = "c1" });
                AddOrUpdateOrder(new Order() { OrderKey = i, UserKey = i, Orderdate = new DateTime(2000, 1, 1, 0, 0, 0), Money = i });
            }
        }

        private static IEnumerable<RowNumberResult> AscendingRowNumbers()
        {
            for (int i = 1; i <= NullOrderingRowCount; i++)
            {
                yield return new RowNumberResult("c1", i, i);
            }
        }

        private static IEnumerable<RowNumberResult> DescendingRowNumbers()
        {
            for (int i = 1; i <= NullOrderingRowCount; i++)
            {
                yield return new RowNumberResult("c1", i, NullOrderingRowCount - i + 1);
            }
        }

        // Explicit null placement over a joined column, every row matches so no null appears
        [Fact]
        public async Task RowNumberAscNullsLastOverJoinedColumnDoesNotFaultTheStream()
        {
            SeedMatchedUsersAndOrders();

            await StartStream(@"
            INSERT INTO output
            SELECT
                u.CompanyId,
                u.UserKey,
                ROW_NUMBER() OVER (PARTITION BY u.CompanyId ORDER BY o.Money ASC NULLS LAST)
            FROM users u
            LEFT JOIN orders o ON u.UserKey = o.UserKey");
            await WaitForUpdate();

            AssertCurrentDataEqual(AscendingRowNumbers());
        }

        // Control, default null placement over the same joined column
        [Fact]
        public async Task RowNumberPlainAscOverJoinedColumnIsUnaffected()
        {
            SeedMatchedUsersAndOrders();

            await StartStream(@"
            INSERT INTO output
            SELECT
                u.CompanyId,
                u.UserKey,
                ROW_NUMBER() OVER (PARTITION BY u.CompanyId ORDER BY o.Money)
            FROM users u
            LEFT JOIN orders o ON u.UserKey = o.UserKey");
            await WaitForUpdate();

            AssertCurrentDataEqual(AscendingRowNumbers());
        }

        // Desc nulls first must emit one row per user, not duplicates
        [Fact]
        public async Task RowNumberDescNullsFirstOverJoinedColumnDoesNotDuplicateRows()
        {
            SeedMatchedUsersAndOrders();

            await StartStream(@"
            INSERT INTO output
            SELECT
                u.CompanyId,
                u.UserKey,
                ROW_NUMBER() OVER (PARTITION BY u.CompanyId ORDER BY o.Money DESC NULLS FIRST)
            FROM users u
            LEFT JOIN orders o ON u.UserKey = o.UserKey");
            await WaitForUpdate();

            AssertCurrentDataEqual(DescendingRowNumbers());
        }

        // Control, plain desc over the same joined column
        [Fact]
        public async Task RowNumberPlainDescOverJoinedColumnIsUnaffected()
        {
            SeedMatchedUsersAndOrders();

            await StartStream(@"
            INSERT INTO output
            SELECT
                u.CompanyId,
                u.UserKey,
                ROW_NUMBER() OVER (PARTITION BY u.CompanyId ORDER BY o.Money DESC)
            FROM users u
            LEFT JOIN orders o ON u.UserKey = o.UserKey");
            await WaitForUpdate();

            AssertCurrentDataEqual(DescendingRowNumbers());
        }

        // Control, the same left join without a window function
        [Fact]
        public async Task LeftJoinWithoutWindowIsUnaffected()
        {
            SourceBatchSize = 16;
            var expected = new List<JoinResult>();
            for (int i = 1; i <= NullOrderingRowCount; i++)
            {
                AddOrUpdateUser(new User() { UserKey = i, CompanyId = "c1" });
                if (i < NullOrderingRowCount)
                {
                    AddOrUpdateOrder(new Order() { OrderKey = i, UserKey = i, Orderdate = new DateTime(2000, 1, 1, 0, 0, 0), Money = i });
                }
                expected.Add(new JoinResult("c1", i, i < NullOrderingRowCount ? i : null));
            }

            await StartStream(@"
            INSERT INTO output
            SELECT u.CompanyId, u.UserKey, o.Money
            FROM users u
            LEFT JOIN orders o ON u.UserKey = o.UserKey");
            await WaitForUpdate();

            AssertCurrentDataEqual(expected);
        }

        // Control, asc nulls last over a plain nullable column
        [Fact]
        public async Task RowNumberAscNullsLastWithoutJoinIsUnaffected()
        {
            SourceBatchSize = 16;
            for (int i = 1; i <= NullOrderingRowCount; i++)
            {
                AddOrUpdateUser(new User() { UserKey = i, CompanyId = "c1", Visits = i < NullOrderingRowCount ? i : null });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                u.CompanyId,
                u.UserKey,
                ROW_NUMBER() OVER (PARTITION BY u.CompanyId ORDER BY u.Visits ASC NULLS LAST)
            FROM users u");
            await WaitForUpdate();

            AssertCurrentDataEqual(AscendingRowNumbers());
        }

        // Values where accumulating from the partition start and reseeding land on different ulps
        private static readonly double[] StabilityValues = { 6.7, 1.4, 1.3, 5.2, 1.7, 2.6, 7.2, 5.1 };

        private void AddStabilityPartition()
        {
            for (int i = 0; i < StabilityValues.Length; i++)
            {
                AddNamedUser(i, StabilityValues[i], "original");
            }
        }

        private Dictionary<int, StabilityRow> CurrentStabilityRows()
        {
            var rows = GetActualRows();
            var result = new Dictionary<int, StabilityRow>();
            for (int i = 0; i < rows.Count; i++)
            {
                var userKey = (int)rows.Columns[1].GetValueAt(i, default).AsLong;
                var firstName = rows.Columns[2].GetValueAt(i, default).AsString.ToString();
                var value = rows.Columns[3].GetValueAt(i, default);
                result[userKey] = new StabilityRow(firstName, value.IsNull ? null : value.AsDouble);
            }
            return result;
        }

        private static void AssertWindowValuesUnchanged(Dictionary<int, StabilityRow> before, Dictionary<int, StabilityRow> after)
        {
            foreach (var row in before.OrderBy(x => x.Key))
            {
                Assert.True(after.ContainsKey(row.Key), $"UserKey {row.Key} is missing from the output after the update");
                Assert.True(row.Value.windowValue == after[row.Key].windowValue,
                    $"UserKey {row.Key} window value changed from {row.Value.windowValue:R} to {after[row.Key].windowValue:R} after an update that cannot affect its frame");
            }
        }

        // An update outside the frame must not change other rows' sums
        [Fact]
        public async Task BoundedSumValueIsStableAcrossUnrelatedUpdate()
        {
            AddStabilityPartition();

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                FirstName,
                SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            var before = CurrentStabilityRows();

            // Only FirstName changes, the value and the position stay the same
            AddNamedUser(3, 5.2, "renamed");
            await WaitForUpdate();
            var after = CurrentStabilityRows();

            // Guards against a vacuous pass where the update never reached the window
            Assert.Equal("renamed", after[3].firstName);
            AssertWindowValuesUnchanged(before, after);
        }

        /// <summary>
        /// Two nulls empty the frame, so the accumulator holds only float residue at that row and
        /// its output is null. A rescan seeded from that null must land on the same value.
        /// </summary>
        [Fact]
        public async Task BoundedSumValueIsStableAfterAnEmptyFrame()
        {
            AddNamedUser(10, 6.7, "original");
            AddNamedUser(20, 1.4, "original");
            AddNamedUser(30, 9.9, "original");
            AddNamedUser(40, 9.9, "original");
            AddNamedUser(50, 5.2, "original");
            AddNamedUser(60, 1.7, "original");

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                FirstName,
                SUM(CASE WHEN UserKey = 30 THEN NULL WHEN UserKey = 40 THEN NULL ELSE DoubleValue END)
                    OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            var before = CurrentStabilityRows();

            // Directly after the emptied frame, so its rescan seeds from the null output
            AddNamedUser(50, 5.2, "renamed");
            await WaitForUpdate();
            var after = CurrentStabilityRows();

            Assert.Equal("renamed", after[50].firstName);
            AssertWindowValuesUnchanged(before, after);
        }

        // Same stability rule for the bounded average
        [Fact]
        public async Task BoundedAverageValueIsStableAcrossUnrelatedUpdate()
        {
            AddStabilityPartition();

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                FirstName,
                AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            var before = CurrentStabilityRows();

            AddNamedUser(3, 5.2, "renamed");
            await WaitForUpdate();
            var after = CurrentStabilityRows();

            Assert.Equal("renamed", after[3].firstName);
            AssertWindowValuesUnchanged(before, after);
        }

        public record JoinRowNumberResult(int orderkey, long rowNumber);
        public record FrameSumResult(int userkey, double? value);
        public record FrameMinByResult(int userkey, int? best);

        // Unmatched rows arrive null padded, compare must stay reflexive
        // Only reproduces under full suite load
        [Fact]
        public async Task LeftJoinNullPaddingDoesNotCorruptWindowSort()
        {
            AddOrUpdateUser(new User() { UserKey = 1, CompanyId = "c1" });
            AddOrUpdateUser(new User() { UserKey = 2, CompanyId = "c1" });
            AddOrUpdateUser(new User() { UserKey = 3, CompanyId = "c2" });

            AddOrUpdateOrder(new Order() { OrderKey = 1, UserKey = 1 });
            AddOrUpdateOrder(new Order() { OrderKey = 2, UserKey = 2 });
            AddOrUpdateOrder(new Order() { OrderKey = 3, UserKey = 3 });

            // No matching user, enough rows to force a reorder
            var expected = new List<JoinRowNumberResult>()
            {
                new JoinRowNumberResult(1, 1),
                new JoinRowNumberResult(2, 2),
                new JoinRowNumberResult(3, 1),
            };
            for (int i = 0; i < 200; i++)
            {
                var orderKey = 100 + i;
                AddOrUpdateOrder(new Order() { OrderKey = orderKey, UserKey = 999 });
                expected.Add(new JoinRowNumberResult(orderKey, i + 1));
            }

            await StartStream(@"
                INSERT INTO output
                SELECT o.OrderKey,
                ROW_NUMBER() OVER (PARTITION BY u.CompanyId ORDER BY o.OrderKey + 1)
                FROM orders o
                LEFT JOIN users u ON u.UserKey = o.UserKey");
            await WaitForUpdate();

            AssertCurrentDataEqual(expected);
        }

        // An unreachable frame is null, not a ring overflow
        [Fact]
        public async Task HugePrecedingUpperBoundProducesNullsInsteadOfCrashing()
        {
            for (int i = 0; i < 5; i++)
            {
                AddUser("c1", i, i);
            }

            await StartStream(@"
                INSERT INTO output
                SELECT UserKey,
                SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND 9223372036854775807 PRECEDING)
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(new List<FrameSumResult>()
            {
                new FrameSumResult(0, null),
                new FrameSumResult(1, null),
                new FrameSumResult(2, null),
                new FrameSumResult(3, null),
                new FrameSumResult(4, null),
            });
        }

        // A huge offset means unbounded preceding
        // Passes today, the cost is memory not results
        [Fact]
        public async Task MinByHugePrecedingOffsetBehavesAsUnbounded()
        {
            AddUser("c1", 0, 50);
            AddUser("c1", 1, 10);
            AddUser("c1", 2, 30);
            AddUser("c1", 3, 20);

            await StartStream(@"
                INSERT INTO output
                SELECT UserKey,
                min_by(UserKey, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 9223372036854775807 PRECEDING AND CURRENT ROW)
                FROM users");
            await WaitForUpdate();

            // The smallest value over the whole prefix
            AssertCurrentDataEqual(new List<FrameMinByResult>()
            {
                new FrameMinByResult(0, 0),
                new FrameMinByResult(1, 1),
                new FrameMinByResult(2, 1),
                new FrameMinByResult(3, 1),
            });
        }
    }
}
