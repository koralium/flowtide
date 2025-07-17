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

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    /// <summary>
    /// This class handles reading replication details from a publication.
    /// It allows reading replication details from multiple tables and notificies all subscribers of the changes.
    /// </summary>
    internal class PostgresReplicationHandler
    {
        private readonly string connectionString;
        private readonly string pubName;
        private IAsyncEnumerator<PgOutputReplicationMessage>? enumerator;
        private string replicationSlotName;

        public PostgresReplicationHandler(string connectionString, string pubName)
        {
            this.connectionString = connectionString;
            this.pubName = pubName;
            replicationSlotName = Guid.NewGuid().ToString();
        }

        public void Subscribe(string tableName, long walLocation)
        {

        }

        public async Task Initialize()
        {
            
            //await foreach (var message in conn.StartReplication(slot, new PgOutputReplicationOptions("", PgOutputProtocolVersion.V1), default)) 
            //{
            //    if (message is InsertMessage insertMessage)
            //    {
            //        await foreach(var value in insertMessage.NewRow)
            //        {
                        
            //        }
            //    }
            //}
        }

        private async Task RunReplication()
        {
            await using var conn = new LogicalReplicationConnection(connectionString);
            var replicationSlot = await conn.CreatePgOutputReplicationSlot(replicationSlotName, true);
            enumerator = conn.StartReplication(replicationSlot, new PgOutputReplicationOptions(pubName, PgOutputProtocolVersion.V1), default, new NpgsqlTypes.NpgsqlLogSequenceNumber(1)).GetAsyncEnumerator();

            while (await enumerator.MoveNextAsync())
            {
                enumerator.Current.WalStart
            }
        }
    }
}
