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

using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests
{
    public class PlanEquality
    {
        readonly Plan root;
        readonly Plan clone;
        readonly Plan notEqual;

        public PlanEquality()
        {
            root = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Emit = new List<int> { 1, 2, 3 },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() { "c1" },
                            },
                            NamedTable = new Type.NamedTable
                            {
                                Names = new List<string>() { "t1" }
                            }
                        },
                        Names = new List<string>() { "c1" }
                    }
                }
            };
            clone = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Emit = new List<int> { 1, 2, 3 },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() { "c1" },
                            },
                            NamedTable = new Type.NamedTable
                            {
                                Names = new List<string>() { "t1" }
                            }
                        },
                        Names = new List<string>() { "c1" }
                    }
                }
            };
            notEqual = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Emit = new List<int> { 1, 2, 3, 4 },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() { "c2" },
                            },
                            NamedTable = new Type.NamedTable
                            {
                                Names = new List<string>() { "t2" }
                            }
                        },
                        Names = new List<string>() { "c2" }
                    }
                }
            };
        }

        [Fact]
        public void IsEqual()
        {
            Assert.Equal(root, clone);
        }

        [Fact]
        public void IsNotEqual()
        {
            Assert.NotEqual(root, notEqual);
        }

        [Fact]
        public void HashCodeIsEqual()
        {
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
        }

        [Fact]
        public void RelationsChangedNotEqual()
        {
            clone.Relations = notEqual.Relations;
            Assert.NotEqual(root, clone);
        }
    }
}
