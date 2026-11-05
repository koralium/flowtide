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
using FlowtideDotNet.AcceptanceTests.Entities;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Text.RegularExpressions;

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
            await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);
            await schemaServiceClient.ReadSchemaAsync(new ReadSchemaRequest(), metadata);

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
        public async Task TestNullKeyRowsSkippedWithWarning()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", $"Bearer {nameof(TestNullKeyRowsSkippedWithWarning)}");
            await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);

            var logs = new TestLogCollector();
            var stream = new SpiceDbTestStream(nameof(TestNullKeyRowsSkippedWithWarning), spiceDbFixture.GetChannel(), true, false);
            stream.AddLoggerProvider(logs);
            stream.Generate(20);

            // One null per required key column
            var nullKeyOrders = stream.Orders.Take(5).Select(x => x.OrderKey).ToList();
            var becomesNull = stream.Orders[5];

            // Null subject_relation must write a direct subject
            await stream.StartStream($@"
                INSERT INTO spicedb
                SELECT
                CASE WHEN orderkey = {nullKeyOrders[0]} THEN null ELSE 'user' END as subject_type,
                CASE WHEN orderkey = {nullKeyOrders[1]} OR userkey = -1 THEN null ELSE userkey END as subject_id,
                null as subject_relation,
                CASE WHEN orderkey = {nullKeyOrders[2]} THEN null ELSE 'reader' END as relation,
                CASE WHEN orderkey = {nullKeyOrders[3]} THEN null ELSE 'document' END as resource_type,
                CASE WHEN orderkey = {nullKeyOrders[4]} THEN null ELSE orderkey END as resource_id
                FROM orders
            ");

            var permissionService = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            async Task<HashSet<string>> ReadAll()
            {
                var relationships = await permissionService.ReadRelationships(new ReadRelationshipsRequest()
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
                return relationships
                    .Select(x => $"{x.Relationship.Resource.ObjectId}#{x.Relationship.Relation}@{x.Relationship.Subject.Object.ObjectType}:{x.Relationship.Subject.Object.ObjectId}#{x.Relationship.Subject.OptionalRelation}")
                    .ToHashSet();
            }

            int TotalSkipped()
            {
                return logs.Lines(LogLevel.Warning, "rows with a null key column")
                    .Sum(x => int.Parse(Regex.Match(x, @"Skipped (\d+) rows").Groups[1].Value));
            }

            var expected = stream.Orders.Skip(5).Select(x => $"{x.OrderKey}#reader@user:{x.UserKey}#").ToHashSet();

            var stopwatch = Stopwatch.StartNew();
            HashSet<string> actual;
            while (true)
            {
                actual = await ReadAll();
                if (actual.SetEquals(expected))
                {
                    break;
                }
                if (stopwatch.ElapsedMilliseconds > 10000)
                {
                    Assert.Fail($"Initial relationships did not match, current count: {actual.Count}");
                }
                await Task.Delay(10);
            }

            // Mixed initial batch gives one warning
            var initialWarning = Assert.Single(logs.Lines(LogLevel.Warning, "rows with a null key column"));
            Assert.Contains("Skipped 5 rows", initialWarning);
            Assert.Matches(@"example: `[^`]*null[^`]*`", initialWarning);

            // Valid row turns null, null key row deleted
            stream.AddOrUpdateOrder(new Order()
            {
                OrderKey = becomesNull.OrderKey,
                UserKey = -1,
                Orderdate = becomesNull.Orderdate,
                GuidVal = becomesNull.GuidVal,
                Money = becomesNull.Money
            });
            stream.DeleteOrder(stream.Orders.First(x => x.OrderKey == nullKeyOrders[0]));
            expected.Remove($"{becomesNull.OrderKey}#reader@user:{becomesNull.UserKey}#");

            stopwatch.Restart();
            while (true)
            {
                await stream.SchedulerTick();
                actual = await ReadAll();
                if (actual.SetEquals(expected) && TotalSkipped() == 7)
                {
                    break;
                }
                if (stopwatch.ElapsedMilliseconds > 10000)
                {
                    Assert.Fail($"Update did not apply, relationship count: {actual.Count}, skipped: {TotalSkipped()}");
                }
                await Task.Delay(10);
            }

            Assert.Empty(logs.Lines(LogLevel.Warning, "Failed to write relationships"));
        }

        [Fact]
        public async Task TestSubjectRelationWriteAndDelete()
        {
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", $"Bearer {nameof(TestSubjectRelationWriteAndDelete)}");
            await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = @"
                    definition user {}

                    definition group {
                        relation member: user
                        relation admin: user
                    }

                    definition folder {
                        relation viewer: user | group#member | group#admin
                    }"
            }, metadata);

            var logs = new TestLogCollector();
            var stream = new SpiceDbTestStream(nameof(TestSubjectRelationWriteAndDelete), spiceDbFixture.GetChannel(), true, false);
            stream.AddLoggerProvider(logs);
            stream.Generate(20);

            // Rows differ only by subject_relation
            await stream.StartStream(@"
                INSERT INTO spicedb
                SELECT 'group' as subject_type, userkey as subject_id, 'member' as subject_relation, 'viewer' as relation, 'folder' as resource_type, orderkey as resource_id
                FROM orders
                UNION ALL
                SELECT 'group' as subject_type, userkey as subject_id, 'admin' as subject_relation, 'viewer' as relation, 'folder' as resource_type, orderkey as resource_id
                FROM orders
            ");

            var permissionService = new PermissionsService.PermissionsServiceClient(spiceDbFixture.GetChannel());

            async Task<HashSet<string>> ReadAll()
            {
                var relationships = await permissionService.ReadRelationships(new ReadRelationshipsRequest()
                {
                    RelationshipFilter = new RelationshipFilter()
                    {
                        ResourceType = "folder"
                    },
                    Consistency = new Consistency()
                    {
                        FullyConsistent = true
                    }
                }, metadata).ResponseStream.ReadAllAsync().ToListAsync();
                return relationships
                    .Select(x => $"{x.Relationship.Resource.ObjectId}#{x.Relationship.Relation}@{x.Relationship.Subject.Object.ObjectType}:{x.Relationship.Subject.Object.ObjectId}#{x.Relationship.Subject.OptionalRelation}")
                    .ToHashSet();
            }

            static IEnumerable<string> Both(int orderKey, int userKey)
            {
                yield return $"{orderKey}#viewer@group:{userKey}#member";
                yield return $"{orderKey}#viewer@group:{userKey}#admin";
            }

            var expected = stream.Orders.SelectMany(x => Both(x.OrderKey, x.UserKey)).ToHashSet();

            var stopwatch = Stopwatch.StartNew();
            HashSet<string> actual;
            while (true)
            {
                actual = await ReadAll();
                if (actual.SetEquals(expected))
                {
                    break;
                }
                if (stopwatch.ElapsedMilliseconds > 10000)
                {
                    Assert.Fail($"Initial relationships did not match, expected: {expected.Count}, current count: {actual.Count}");
                }
                await Task.Delay(10);
            }

            // Deletes must target the userset relationships
            var deleted = stream.Orders[0];
            var moved = stream.Orders[1];
            var movedToUser = moved.UserKey + 100000;
            stream.DeleteOrder(deleted);
            stream.AddOrUpdateOrder(new Order()
            {
                OrderKey = moved.OrderKey,
                UserKey = movedToUser,
                Orderdate = moved.Orderdate,
                GuidVal = moved.GuidVal,
                Money = moved.Money
            });
            expected.ExceptWith(Both(deleted.OrderKey, deleted.UserKey));
            expected.ExceptWith(Both(moved.OrderKey, moved.UserKey));
            expected.UnionWith(Both(moved.OrderKey, movedToUser));

            stopwatch.Restart();
            while (true)
            {
                await stream.SchedulerTick();
                actual = await ReadAll();
                if (actual.SetEquals(expected))
                {
                    break;
                }
                if (stopwatch.ElapsedMilliseconds > 10000)
                {
                    Assert.Fail($"Update did not apply, expected: {expected.Count}, current count: {actual.Count}");
                }
                await Task.Delay(10);
            }

            Assert.Empty(logs.Lines(LogLevel.Warning, "Failed to write relationships"));
            Assert.Empty(logs.Lines(LogLevel.Warning, "rows with a null key column"));
        }

        [Fact]
        public async Task TestRead()
        {
            var schemaText = File.ReadAllText("schema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata();
            metadata.Add("Authorization", $"Bearer {nameof(TestRead)}");
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
            await schemaServiceClient.WriteSchemaAsync(new WriteSchemaRequest()
            {
                Schema = schemaText
            }, metadata);

            var readRequest = ISpiceDbReadRelationshipsRequest.Create(
                 ISpiceDbRelationshipFilter.Create(resourceType: "document", optionalRelation: "reader"),
                 consistency: ISpiceDbConsistency.CreateFullyConsistent()
                );

            var stream = new SpiceDbTestStream(
                nameof(TestInsertDeleteExisting),
                spiceDbFixture.GetChannel(),
                true,
                false,
                readRequest);
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

        [Fact]
        public async Task UseTableFunctionForDenormalizeNoStopTypes()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", $"Bearer {nameof(UseTableFunctionForDenormalizeNoStopTypes)}" }
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

            var stream = new SpiceDbTestStream(nameof(UseTableFunctionForDenormalizeNoStopTypes), spiceDbFixture.GetChannel(), false, true);

            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    subject_type,
                    subject_id,
                    relation,
                    resource_type,
                    resource_id
                FROM spicedb.materialize_permission('organization', 'can_view')
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
        public async Task UseTableFunctionForDenormalizeWithStopType()
        {
            var schemaText = File.ReadAllText("recursiveschema.txt");
            SchemaService.SchemaServiceClient schemaServiceClient = new SchemaService.SchemaServiceClient(spiceDbFixture.GetChannel());

            var metadata = new Metadata
            {
                { "Authorization", $"Bearer {nameof(UseTableFunctionForDenormalizeWithStopType)}" }
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

            var stream = new SpiceDbTestStream(nameof(UseTableFunctionForDenormalizeWithStopType), spiceDbFixture.GetChannel(), false, true);

            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    subject_type,
                    subject_id,
                    relation,
                    resource_type,
                    resource_id
                FROM spicedb.materialize_permission('project', 'can_view_wildcard', false, 'organization')
            ");

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual([
                new { subjectType = "organization", subjectId = "1", relation = "can_view_wildcard", resourceType = "project", resourceId = "123"},
                 new { subjectType = "user", subjectId = "*", relation = "can_view_wildcard", resourceType = "project", resourceId = "123"},
                ]);
        }
    }
}