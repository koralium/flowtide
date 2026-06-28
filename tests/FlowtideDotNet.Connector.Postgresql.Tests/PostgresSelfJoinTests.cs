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

using FlowtideDotNet.Connector.PostgreSQL;

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    public partial class PostgresSourceTests
    {
        [Fact]
        public async Task SelfJoinSharedMode()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.selfjoin_s");
            await _fixture.ExecuteAsync("CREATE TABLE public.selfjoin_s (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.selfjoin_s VALUES (1, 'x')");

            await using var stream = CreateStream(nameof(SelfJoinSharedMode), PostgresReplicationMode.Shared);
            await stream.StartStream(@"INSERT INTO output
                SELECT a.id, b.name FROM public.selfjoin_s a JOIN public.selfjoin_s b ON a.id = b.id");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "x" } });

            // A delta must reach BOTH source operators (both sides of the self-join) for the join to update.
            await _fixture.ExecuteAsync("UPDATE public.selfjoin_s SET name = 'y' WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "y" } });
        }

        [Fact]
        public async Task SelfJoinPerTableMode()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.selfjoin_p");
            await _fixture.ExecuteAsync("CREATE TABLE public.selfjoin_p (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.selfjoin_p VALUES (1, 'x')");

            await using var stream = CreateStream(nameof(SelfJoinPerTableMode), PostgresReplicationMode.PerTable);
            await stream.StartStream(@"INSERT INTO output
                SELECT a.id, b.name FROM public.selfjoin_p a JOIN public.selfjoin_p b ON a.id = b.id");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "x" } });

            await _fixture.ExecuteAsync("UPDATE public.selfjoin_p SET name = 'y' WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "y" } });
        }
    }
}
