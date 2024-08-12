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

using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Substrait.Tests
{
    public class SerializeTests
    {
        [Fact]
        public void SerializeWriteAndRead()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);
                create table table2 (b any);
                
                insert into out
                select lower(a) FROM table1 t1
                LEFT JOIN table2 t2 ON t1.a = t2.b;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            var protoPlan = SubstraitSerializer.Serialize(plan);
            
            var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                CustomProtobuf.IterationRelation.Descriptor,
                CustomProtobuf.NormalizationRelation.Descriptor);
            var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                .WithIndentation();
            var formatter = new Google.Protobuf.JsonFormatter(settings);
            var json = formatter.Format(protoPlan);
        }

        [Fact]
        public void SerializeTopNRelation()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);
                
                insert into out
                select TOP (1) a FROM table1 t1 ORDER BY a
            ");
            var plan = sqlPlanBuilder.GetPlan();

            var ex = Record.Exception(() =>
            {
                var protoPlan = SubstraitSerializer.Serialize(plan);

                var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                    CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                    CustomProtobuf.IterationRelation.Descriptor,
                    CustomProtobuf.NormalizationRelation.Descriptor);
                var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                    .WithIndentation();
                var formatter = new Google.Protobuf.JsonFormatter(settings);
                var json = formatter.Format(protoPlan);
            });
            Assert.Null(ex);
        }

        [Fact]
        public void SerializeTableFunctionInFrom()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                insert into out
                select a FROM UNNEST(list(1,2,3)) a
            ");
            var plan = sqlPlanBuilder.GetPlan();

            var ex = Record.Exception(() =>
            {
                var protoPlan = SubstraitSerializer.Serialize(plan);

                var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                    CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                    CustomProtobuf.IterationRelation.Descriptor,
                    CustomProtobuf.NormalizationRelation.Descriptor);
                var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                    .WithIndentation();
                var formatter = new Google.Protobuf.JsonFormatter(settings);
                var json = formatter.Format(protoPlan);
            });
            Assert.Null(ex);
        }

        [Fact]
        public void SerializeTableFunctionInJoin()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);

                insert into out
                select t.a FROM table1 t
                LEFT JOIN UNNEST(t.a) c ON c = 123
            ");
            var plan = sqlPlanBuilder.GetPlan();

            var ex = Record.Exception(() =>
            {
                var protoPlan = SubstraitSerializer.Serialize(plan);

                var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                    CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                    CustomProtobuf.IterationRelation.Descriptor,
                    CustomProtobuf.NormalizationRelation.Descriptor);
                var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                    .WithIndentation();
                var formatter = new Google.Protobuf.JsonFormatter(settings);
                var json = formatter.Format(protoPlan);
            });
            Assert.Null(ex);
        }
    }
}
