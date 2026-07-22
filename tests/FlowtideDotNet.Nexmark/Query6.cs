using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark
{
    public class Query6 : QueryBase
    {
        [Benchmark]
        public async Task Q6()
        {
            await Stream.StartStream(@"
            INSERT INTO output
            SELECT
                Q.seller,
                AVG(Q.price) OVER
                    (PARTITION BY Q.seller ORDER BY Q.date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS avgprice
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id, seller ORDER BY price DESC) AS rownum
                FROM (SELECT A.id, A.seller, B.price, B.date_time
                    FROM auction AS A,
                        bid AS B
                    WHERE A.id = B.auction
                        and B.date_time between A.dateTime and A.expires) AS J
                WHERE rownum <= 1
            ) AS Q;
            ", planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings()
            {
                Parallelization = 1
            });
            await Stream.WaitForUpdate();
        }
    }
}
