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

using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    public class SubstreamGroupResolverTests
    {
        private static Plan BuildPlan(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql("CREATE TABLE users (userkey any);");
            builder.Sql(sql);
            return builder.GetPlan();
        }

        private const string Chain = @"
            SUBSTREAM sub1;
            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            SUBSTREAM sub2;
            CREATE VIEW v2 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM v1 WITH (PARTITION_ID = 0);

            SUBSTREAM sub3;
            INSERT INTO output SELECT userkey FROM v2 WITH (PARTITION_ID = 0);
            ";

        [Theory]
        [InlineData("sub1", new[] { "sub2" })]
        [InlineData("sub2", new[] { "sub1", "sub3" })]
        [InlineData("sub3", new[] { "sub2" })]
        public void AChainCountsEverySubstreamButOnlyNeighboursArePeers(string self, string[] expectedPeers)
        {
            var group = SubstreamGroupResolver.Resolve(BuildPlan(Chain), self);

            Assert.Equal(3, group.GroupSize);
            Assert.Equal(2, group.Distance);
            Assert.Equal(expectedPeers.OrderBy(x => x), group.Peers.OrderBy(x => x));
        }

        [Fact]
        public void TheProducerAndTheReaderSeeTheSameEdge()
        {
            var plan = BuildPlan(@"
            SUBSTREAM sub1;
            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;
            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            SUBSTREAM sub2;
            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ");

            var first = SubstreamGroupResolver.Resolve(plan, "sub1");
            var second = SubstreamGroupResolver.Resolve(plan, "sub2");

            Assert.Equal(new[] { "sub2" }, first.Peers);
            Assert.Equal(new[] { "sub1" }, second.Peers);
            Assert.Equal(2, first.GroupSize);
            Assert.Equal(1, first.Distance);
            Assert.Equal(2, second.GroupSize);
        }

        [Fact]
        public void SubstreamsThatNeverExchangeDataAreSeparateGroups()
        {
            var plan = BuildPlan(@"
            SUBSTREAM sub1;
            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            SUBSTREAM sub2;
            INSERT INTO output SELECT userkey FROM v1 WITH (PARTITION_ID = 0);

            SUBSTREAM sub3;
            CREATE VIEW v3 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            SUBSTREAM sub4;
            INSERT INTO output2 SELECT userkey FROM v3 WITH (PARTITION_ID = 0);
            ");

            Assert.Equal(2, SubstreamGroupResolver.Resolve(plan, "sub1").GroupSize);
            Assert.Equal(new[] { "sub4" }, SubstreamGroupResolver.Resolve(plan, "sub3").Peers);
            Assert.Equal(2, SubstreamGroupResolver.Resolve(plan, "sub4").GroupSize);
        }

        [Fact]
        public void ASubstreamWithoutExchangesAgreesWithItself()
        {
            var plan = BuildPlan(@"
            SUBSTREAM sub1;
            INSERT INTO output SELECT userkey FROM users;
            ");

            var group = SubstreamGroupResolver.Resolve(plan, "sub1");

            Assert.Empty(group.Peers);
            Assert.Equal(1, group.GroupSize);
        }

        /// <summary>
        /// What the automatic distribution makes of a join: the scans scatter to every copy and the copies gather to the sink.
        /// </summary>
        [Theory]
        [InlineData(2, 1)]
        [InlineData(4, 2)]
        [InlineData(8, 2)]
        public void AnAutomaticallyDistributedJoinStaysTwoHopsWide(int substreamCount, int expectedDistance)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
            CREATE TABLE users (userkey any);
            CREATE TABLE orders (orderkey any, userkey any);

            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ");
            var plan = FlowtideDotNet.Core.Optimizer.PlanOptimizer.Optimize(builder.GetPlan(), new FlowtideDotNet.Core.Optimizer.PlanOptimizerSettings()
            {
                DistributedPlanOptions = new FlowtideDotNet.Core.Optimizer.DistributedMode.DistributedPlanOptions()
                {
                    SubstreamCount = substreamCount
                }
            });

            for (int i = 0; i < substreamCount; i++)
            {
                var group = SubstreamGroupResolver.Resolve(plan, $"substream_{i}");
                Assert.Equal(substreamCount, group.GroupSize);
                Assert.Equal(expectedDistance, group.Distance);
            }
        }

        [Fact]
        public void AnUnknownSubstreamAgreesWithItself()
        {
            var group = SubstreamGroupResolver.Resolve(BuildPlan(Chain), "somewhere_else");

            Assert.Empty(group.Peers);
            Assert.Equal(1, group.GroupSize);
        }
    }
}
