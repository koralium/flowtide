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

using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    internal static class PostgresClientUtils
    {
        public static async Task<NpgsqlLogSequenceNumber> GetCurrentWalLocation(NpgsqlConnection connection)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_current_wal_lsn()";

            var result = await cmd.ExecuteScalarAsync();
            if (result is NpgsqlLogSequenceNumber lsn)
            {
                return lsn;
            }
            else if (result is string strLsn)
            {
                return NpgsqlLogSequenceNumber.Parse(strLsn);
            }
            else
            {
                throw new InvalidOperationException("Unexpected result type from pg_current_wal_lsn()");
            }
        }
    }
}
