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

using Base.V1;
using PermifyProto = Base.V1;

namespace FlowtideDotNet.Connector.Permify.Tests
{
    public class PermifyTests : IClassFixture<PermifyFixture>
    {
        private readonly PermifyFixture permifyFixture;

        public PermifyTests(PermifyFixture permifyFixture)
        {
            this.permifyFixture = permifyFixture;
        }

        private async Task<string> CreateTenantAsync(string id)
        {
            Tenancy.TenancyClient tenancyClient = new Tenancy.TenancyClient(permifyFixture.GetChannel());
            var createTenantResponse = await tenancyClient.CreateAsync(new TenantCreateRequest()
            {
                Id = id,
                Name = id
            });
            return createTenantResponse.Tenant.Id;
        }

        private async Task WriteDefaultSchema(string tenantId)
        {
            var schema = @"
            entity user {} 

            entity organization {

                relation admin  @user
                relation member @user

                action create_repository = admin or member
            }
            ";

            Schema.SchemaClient schemaClient = new Schema.SchemaClient(permifyFixture.GetChannel());

            await schemaClient.WriteAsync(new SchemaWriteRequest()
            {
                Schema = schema,
                TenantId = tenantId
            });
        }

        [Fact]
        public async Task WriteToPermify()
        {
            var tenantId = await CreateTenantAsync("testwrite");
            await WriteDefaultSchema(tenantId);

            PermifyTestStream stream = new PermifyTestStream("testwrite", tenantId, permifyFixture.GetChannel(), true, false);

            // Use 99 to get 2 batches, 1 with 50 and the last with 49
            stream.Generate(99);

            await stream.StartStream(@"
                INSERT INTO permify
                SELECT
                'user' as subject_type,
                userkey as subject_id,
                'member' as relation,
                'organization' as entity_type,
                orderkey as entity_id
                FROM orders
            ");

            await stream.WaitForUpdate();

            Data.DataClient dataClient = new Data.DataClient(permifyFixture.GetChannel());

            var readResponse = await dataClient.ReadRelationshipsAsync(new RelationshipReadRequest()
            {
                PageSize = 100,
                TenantId = tenantId,
                Metadata = new RelationshipReadRequestMetadata(),
                Filter = new TupleFilter()
            });
            Assert.Equal(99, readResponse.Tuples.Count);

            for (int i = 0; i < stream.Orders.Count; i++)
            {
                var order = stream.Orders[i];
                var tuple = readResponse.Tuples[i];

                Assert.Equal("user", tuple.Subject.Type);
                Assert.Equal(order.UserKey.ToString(), tuple.Subject.Id);
                Assert.Equal("member", tuple.Relation);
                Assert.Equal("organization", tuple.Entity.Type);
                Assert.Equal(order.OrderKey.ToString(), tuple.Entity.Id);
            }

            // Delete the first two orders
            stream.DeleteOrder(stream.Orders[0]);
            stream.DeleteOrder(stream.Orders[0]);
            await stream.WaitForUpdate();

            readResponse = await dataClient.ReadRelationshipsAsync(new RelationshipReadRequest()
            {
                PageSize = 100,
                TenantId = tenantId,
                Metadata = new RelationshipReadRequestMetadata(),
                Filter = new TupleFilter()
            });

            Assert.Equal(97, readResponse.Tuples.Count);

            for (int i = 0; i < stream.Orders.Count; i++)
            {
                var order = stream.Orders[i];
                var tuple = readResponse.Tuples[i];

                Assert.Equal("user", tuple.Subject.Type);
                Assert.Equal(order.UserKey.ToString(), tuple.Subject.Id);
                Assert.Equal("member", tuple.Relation);
                Assert.Equal("organization", tuple.Entity.Type);
                Assert.Equal(order.OrderKey.ToString(), tuple.Entity.Id);
            }
        }

        [Fact]
        public async Task ReadFromPermify()
        {
            var tenantId = await CreateTenantAsync("testread");
            await WriteDefaultSchema(tenantId);

            Data.DataClient dataClient = new Data.DataClient(permifyFixture.GetChannel());

            var writeRequest = new RelationshipWriteRequest()
            {
                Metadata = new RelationshipWriteRequestMetadata(),
                TenantId = tenantId
            };
            writeRequest.Tuples.Add(new PermifyProto.Tuple() {
                Entity = new PermifyProto.Entity()
                {
                    Id = "1",
                    Type = "organization"
                },
                Relation = "admin",
                Subject = new PermifyProto.Subject()
                {
                    Id = "1",
                    Type = "user"
                }
            });
            writeRequest.Tuples.Add(new PermifyProto.Tuple()
            {
                Entity = new PermifyProto.Entity()
                {
                    Id = "1",
                    Type = "organization"
                },
                Relation = "admin",
                Subject = new PermifyProto.Subject()
                {
                    Id = "2",
                    Type = "user"
                }
            });
            await dataClient.WriteRelationshipsAsync(writeRequest);

            PermifyTestStream stream = new PermifyTestStream("testread", tenantId, permifyFixture.GetChannel(), false, true);

            await stream.StartStream(@"
                CREATE TABLE permify (
                    subject_id,
                    subject_type,
                    relation,
                    entity_id,
                    entity_type
                );

                INSERT INTO outdata
                SELECT
                subject_type,
                subject_id,
                relation,
                entity_type,
                entity_id
                FROM permify
            ");

            await stream.WaitForUpdate();

            var actual = stream.GetActualRowsAsVectors();

            // Send an update
            writeRequest = new RelationshipWriteRequest()
            {
                Metadata = new RelationshipWriteRequestMetadata(),
                TenantId = tenantId
            };
            writeRequest.Tuples.Add(new PermifyProto.Tuple()
            {
                Entity = new PermifyProto.Entity()
                {
                    Id = "1",
                    Type = "organization"
                },
                Relation = "admin",
                Subject = new PermifyProto.Subject()
                {
                    Id = "3",
                    Type = "user"
                }
            });
            await dataClient.WriteRelationshipsAsync(writeRequest);

            await stream.WaitForUpdate();

            var actual2 = stream.GetActualRowsAsVectors();
        }
    }
}