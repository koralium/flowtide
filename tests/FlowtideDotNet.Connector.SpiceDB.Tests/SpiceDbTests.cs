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

using Authzed.Api.V1;
using Grpc.Core;
using System.Diagnostics;

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
            metadata.Add("Authorization", $"Bearer {nameof(TestInsert)}");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            var schema = await schemaServiceClient.ReadSchemaAsync(new ReadSchemaRequest(), metadata);

            var stream = new SpiceDbTestStream(nameof(TestInsert), spiceDbFixture.GetChannel(), true, false);
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
                    },
                    Consistency = new Consistency()
                    {
                        FullyConsistent = true
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
        public async Task TestInsertThenDelete()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", $"Bearer {nameof(TestInsertThenDelete)}");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            var schema = await schemaServiceClient.ReadSchemaAsync(new ReadSchemaRequest(), metadata);

            var stream = new SpiceDbTestStream(nameof(TestInsertThenDelete), spiceDbFixture.GetChannel(), true, false);
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
                    },
                    Consistency = new Consistency()
                    {
                        FullyConsistent = true
                    }
                }, metadata).ResponseStream.ReadAllAsync().ToListAsync();

                if (existing.Count >= 1000)
                {
                    break;
                }
                await Task.Delay(10);
            }
            Assert.Equal(1000, existing.Count);

            var firstOrder = stream.Orders[0];
            stream.DeleteOrder(firstOrder);

            while (true)
            {
                await stream.SchedulerTick();
                existing = await permissionService.ReadRelationships(new ReadRelationshipsRequest()
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

                if (existing.Count == 999)
                {
                    break;
                }
                await Task.Delay(10);
            }
            Assert.Equal(999, existing.Count);
        }

        [Fact]
        public async Task TestRead()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", $"Bearer {nameof(TestRead)}");
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

            var stream = new SpiceDbTestStream(nameof(TestRead), spiceDbFixture.GetChannel(), false, true);

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

            // Wait 1 second so it is guaranteed that the data has been updated
            await Task.Delay(1000);

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
            metadata.Add("Authorization", $"Bearer {nameof(TestReadPermissions)}");
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

            var stream = new SpiceDbTestStream(nameof(TestReadPermissions), spiceDbFixture.GetChannel(), false, true);
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
                { "Authorization", $"Bearer {nameof(TestReadPermissionsRecursive)}" }
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

            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Resource = new ObjectReference()
                    {
                        ObjectType = "project",
                        ObjectId = "123"
                    },
                    Relation = "organization",
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "1"
                        }
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "organization", "can_view", "spicedb");

            var stream = new SpiceDbTestStream(nameof(TestReadPermissionsRecursive), spiceDbFixture.GetChannel(), false, true);
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
        public async Task TestReadPermissionsStopTypeRecurse()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", $"Bearer {nameof(TestReadPermissionsStopTypeRecurse)}" }
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

            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Resource = new ObjectReference()
                    {
                        ObjectType = "project",
                        ObjectId = "123"
                    },
                    Relation = "organization",
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "1"
                        }
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "project", "can_view", "spicedb", true, "organization");

            var stream = new SpiceDbTestStream(nameof(TestReadPermissionsStopTypeRecurse), spiceDbFixture.GetChannel(), false, true);
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

            stream.AssertCurrentDataEqual([
                new { subjectType = "organization", subjectId = "2", relation = "can_view", resourceType = "project", resourceId = "123"}
                ]);

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
            Assert.Empty(actual2);
        }

        [Fact]
        public async Task TestReadPermissionsStopTypeRecurseThreeLevels()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", $"Bearer {nameof(TestReadPermissionsStopTypeRecurseThreeLevels)}" }
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
                        ObjectId = "3"
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
                            ObjectId = "3"
                        }
                    },
                    Relation = "parent",
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
                    Resource = new ObjectReference()
                    {
                        ObjectType = "project",
                        ObjectId = "123"
                    },
                    Relation = "organization",
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "1"
                        }
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "project", "can_view", "spicedb", true, "organization");

            var stream = new SpiceDbTestStream(nameof(TestReadPermissionsStopTypeRecurseThreeLevels), spiceDbFixture.GetChannel(), false, true);
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

            var act = stream.GetActualRowsAsVectors();
            stream.AssertCurrentDataEqual([
                new { subjectType = "organization", subjectId = "3", relation = "can_view", resourceType = "project", resourceId = "123"}
                ]);

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
            Assert.Empty(actual2);
        }

        /// <summary>
        /// This test makes sure that when using a starting type as stop type and it has a recursive permission
        /// it does not resolve the recursive of the current type
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestReadPermissionsStopTypeSameAsStartType()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", $"Bearer {nameof(TestReadPermissionsStopTypeSameAsStartType)}" }
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

            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Resource = new ObjectReference()
                    {
                        ObjectType = "project",
                        ObjectId = "123"
                    },
                    Relation = "organization",
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "1"
                        }
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "organization", "can_view", "spicedb", false, "organization");

            var stream = new SpiceDbTestStream(nameof(TestReadPermissionsStopTypeSameAsStartType), spiceDbFixture.GetChannel(), false, true);
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
            stream.AssertCurrentDataEqual([
                new { subjectType = "user", subjectId = "user1", relation = "can_view", resourceType = "organization", resourceId = "2"}
                ]);
        }

        [Fact]
        public async Task TestReadPermissionsStopTypeRecurseWithWildcard()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", $"Bearer {nameof(TestReadPermissionsStopTypeRecurseWithWildcard)}" }
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

            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Resource = new ObjectReference()
                    {
                        ObjectType = "project",
                        ObjectId = "123"
                    },
                    Relation = "organization",
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "organization",
                            ObjectId = "1"
                        }
                    }
                }
            });

            writeRequest.Updates.Add(new RelationshipUpdate()
            {
                Operation = RelationshipUpdate.Types.Operation.Touch,
                Relationship = new Relationship()
                {
                    Resource = new ObjectReference()
                    {
                        ObjectType = "project",
                        ObjectId = "123"
                    },
                    Relation = "all_can_read",
                    Subject = new SubjectReference()
                    {
                        Object = new ObjectReference()
                        {
                            ObjectType = "user",
                            ObjectId = "*"
                        }
                    }
                }
            });
            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);
            var viewPermissionPlan = SpiceDbToFlowtide.Convert(schemaText, "project", "can_view_wildcard", "spicedb", false, "organization");

            var stream = new SpiceDbTestStream(nameof(TestReadPermissionsStopTypeRecurseWithWildcard), spiceDbFixture.GetChannel(), false, true);
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

            var act = stream.GetActualRowsAsVectors();
            stream.AssertCurrentDataEqual([
                new { subjectType = "organization", subjectId = "1", relation = "can_view_wildcard", resourceType = "project", resourceId = "123"},
                 new { subjectType = "user", subjectId = "*", relation = "can_view_wildcard", resourceType = "project", resourceId = "123"},
                ]);
        }

        [Fact]
        public async Task TestInsertDeleteExisting()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", $"Bearer {nameof(TestInsertDeleteExisting)}");
            var res = await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);

            var stream = new SpiceDbTestStream(
                nameof(TestInsertDeleteExisting),
                spiceDbFixture.GetChannel(),
                true,
                false,
                new ReadRelationshipsRequest()
                {
                    RelationshipFilter = new RelationshipFilter()
                    {
                        ResourceType = "document",
                        OptionalRelation = "reader"
                    },
                    Consistency = new Consistency()
                    {
                        FullyConsistent = true
                    }
                });
            stream.Generate(10);

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
                            ObjectId = stream.Orders[0].UserKey.ToString()
                        }
                    },
                    Relation = "reader",
                    Resource = new ObjectReference()
                    {
                        ObjectType = "document",
                        ObjectId = stream.Orders[0].OrderKey.ToString()
                    }
                }
            });

            await permissionClient.WriteRelationshipsAsync(writeRequest, metadata);

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
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
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
                if (stopwatch.ElapsedMilliseconds > 10000)
                {
                    Assert.Fail("Could not reach count of 10, current count: " + existing.Count.ToString());
                }
                await Task.Delay(10);
            }
            Assert.Equal(10, existing.Count);

        }

        [Fact]
        public void NonExistingStopTypeThrowsError()
        {
            var schemaText = File.ReadAllText("schema.txt");
            Assert.Throws<ArgumentException>(() => SpiceDbToFlowtide.Convert(schemaText, "document", "view", "spicedb", false, "nonexisting"));
        }

        [Fact]
        public void NonExistingResourceTypeThrowsError()
        {
            var schemaText = File.ReadAllText("schema.txt");
            Assert.Throws<ArgumentException>(() => SpiceDbToFlowtide.Convert(schemaText, "nonexisting", "view", "spicedb"));
        }

        [Fact]
        public void RelationDoesNotExistOnResourceType()
        {
            var schemaText = File.ReadAllText("schema.txt");
            Assert.Throws<InvalidOperationException>(() => SpiceDbToFlowtide.Convert(schemaText, "document", "nonexisting", "spicedb"));
        }
    }
}