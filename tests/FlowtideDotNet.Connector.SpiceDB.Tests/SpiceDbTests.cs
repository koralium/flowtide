using Authzed.Api.V1;
using Grpc.Core;
using System.Buffers.Text;
using System.Text;

namespace FlowtideDotNet.Connector.SpiceDB.Tests
{
    public class SpiceDbTests : IAsyncLifetime
    {
        private readonly SpiceDbFixture spiceDbFixture;

        public SpiceDbTests()
        {
            this.spiceDbFixture = new SpiceDbFixture();
        }

        public Task DisposeAsync()
        {
            return spiceDbFixture.DisposeAsync();
        }

        public Task InitializeAsync()
        {
            return spiceDbFixture.InitializeAsync();
        }

        [Fact]
        public async Task TestInsert()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", "Bearer somerandomkeyhere");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            var schema = await schemaServiceClient.ReadSchemaAsync(new ReadSchemaRequest(), metadata);

            var stream = new SpiceDbTestStream("testinsert", spiceDbFixture.GetChannel(), true, false);
            stream.Generate(1000);
            await stream.StartStream(@"
                INSERT INTO spicedb
                SELECT
                'user' as subject_type,
                userkey as subject_id,
                'reader' as relation,
                'document' as resource_type,
                orderkey as resource_id
                FROM orders
            ");

            var permissionService = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            List<ReadRelationshipsResponse>? existing;
            while (true)
            {
                existing = await permissionService.ReadRelationships(new ReadRelationshipsRequest()
                {
                    RelationshipFilter = new RelationshipFilter()
                    {
                        ResourceType = "document"
                    }
                }, metadata).ResponseStream.ReadAllAsync().ToListAsync();

                if (existing.Count >= 1000)
                {
                    break;
                }
                await Task.Delay(10);
            }
            Assert.Equal(1000, existing.Count);
            
        }

        [Fact]
        public async Task TestRead()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", "Bearer somerandomkeyhere");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            var permissionClient = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            var writeRequest = new WriteRelationshipsRequest();
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "user1"
                        }
                    },
                    Relation = "reader",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = "doc1"
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);

            var stream = new SpiceDbTestStream("testread", spiceDbFixture.GetChannel(), false, true);

            await stream.StartStream(@"
                CREATE TABLE spicedb (
                    subject_id,
                    subject_type,
                    relation,
                    resource_id,
                    resource_type
                );

                INSERT INTO outdata
                SELECT
                subject_type,
                subject_id,
                relation,
                resource_type,
                resource_id
                FROM spicedb
                WHERE resource_type = 'document' AND relation = 'reader'
            ");
            await stream.WaitForUpdate();
            var actual = stream.GetActualRowsAsVectors();
            Assert.Single(actual);

            writeRequest = new WriteRelationshipsRequest();
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "user2"
                        }
                    },
                    Relation = "reader",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = "doc1"
                    }
                }
            });
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "user3"
                        }
                    },
                    Relation = "writer",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = "doc1"
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);

            await stream.WaitForUpdate();
            var actual2 = stream.GetActualRowsAsVectors();
            Assert.Equal(2, actual2.Count);
        }

        [Fact]
        public async Task TestReadPermissions()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", "Bearer somerandomkeyhere");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            var permissionClient = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            var writeRequest = new WriteRelationshipsRequest();
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "user1"
                        }
                    },
                    Relation = "reader",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = "doc1"
                    }
                }
            });
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "user2"
                        }
                    },
                    Relation = "writer",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = "doc1"
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "document", "view", "spicedb");

            var stream = new SpiceDbTestStream("testreadpermissions", spiceDbFixture.GetChannel(), false, true);
            stream.SqlPlanBuilder.AddPlanAsView("authdata", viewPermissionPlan);

            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    subject_type,
                    subject_id,
                    relation,
                    resource_type,
                    resource_id
                FROM authdata
            ");
            
            await stream.WaitForUpdate();
            var actual = stream.GetActualRowsAsVectors();
            Assert.Equal(2, actual.Count);
        }

        [Fact]
        public async Task TestReadPermissionsRecursive()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", "Bearer somerandomkeyhere" }
            };
            await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            var permissionClient = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            var writeRequest = new WriteRelationshipsRequest();
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "*"
                        }
                    },
                    Relation = "can_view",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "role",
                        ObjectId = "1"
                    }
                }
            });
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "user1"
                        }
                    },
                    Relation = "user",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "role_binding",
                        ObjectId = "1_1"
                    }
                }
            });
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "role",
                            ObjectId = "1"
                        }
                    },
                    Relation = "role",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "role_binding",
                        ObjectId = "1_1"
                    }
                }
            });
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "role_binding",
                            ObjectId = "1_1"
                        }
                    },
                    Relation = "role_binding",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "organization",
                        ObjectId = "2"
                    }
                }
            });
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "2"
                        }
                    },
                    Relation = "parent",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "organization",
                        ObjectId = "1"
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "organization", "can_view", "spicedb");

            var stream = new SpiceDbTestStream("testreadpermissionsrecursive", spiceDbFixture.GetChannel(), false, true);
            stream.SqlPlanBuilder.AddPlanAsView("authdata", viewPermissionPlan);

            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    subject_type,
                    subject_id,
                    relation,
                    resource_type,
                    resource_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var actual = stream.GetActualRowsAsVectors();
            Assert.Equal(2, actual.Count);

            // Remove the parent connection
            writeRequest = new WriteRelationshipsRequest();
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Delete,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "2"
                        }
                    },
                    Relation = "parent",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "organization",
                        ObjectId = "1"
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            await stream.WaitForUpdate();
            var actual2 = stream.GetActualRowsAsVectors();
            Assert.Single(actual2);
        }

        [Fact]
        public async Task TestInsertDeleteExisting()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", "Bearer somerandomkeyhere");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);

            var permissionClient = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            var writeRequest = new WriteRelationshipsRequest();
            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "9999"
                        }
                    },
                    Relation = "reader",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = "9999"
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);

            var stream = new SpiceDbTestStream(
                "testinsertdeleteexisting", 
                spiceDbFixture.GetChannel(), 
                true, 
                false,
                new ReadRelationshipsRequest()
                {
                    RelationshipFilter = new RelationshipFilter()
                    {
                        ResourceType = "document",
                        OptionalRelation = "reader"
                    }
                });
            stream.Generate(10);
            await stream.StartStream(@"
                INSERT INTO spicedb
                SELECT
                'user' as subject_type,
                userkey as subject_id,
                'reader' as relation,
                'document' as resource_type,
                orderkey as resource_id
                FROM orders
            ");

            List<ReadRelationshipsResponse>? existing;
            while (true)
            {
                existing = await permissionClient.ReadRelationships(new ReadRelationshipsRequest()
                {
                    RelationshipFilter = new RelationshipFilter()
                    {
                        ResourceType = "document"
                    },
                    Consistency = new Consistency()
                    {
                        FullyConsistent = true
                    }
                }, metadata).ResponseStream.ReadAllAsync().ToListAsync();

                if (existing.Count == 10)
                {
                    break;
                }
                await Task.Delay(10);
            }
            Assert.Equal(10, existing.Count);

        }
    }
}