﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            var protoPlan = new SubstraitSerializer().Serialize(plan);

            
            var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                CustomProtobuf.IterationRelation.Descriptor,
                CustomProtobuf.NormalizationRelation.Descriptor,
                CustomProtobuf.ReferenceRelation.Descriptor);
            var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                .WithIndentation();
            var formatter = new Google.Protobuf.JsonFormatter(settings);
            var json = formatter.Format(protoPlan);
        }
    }
}
