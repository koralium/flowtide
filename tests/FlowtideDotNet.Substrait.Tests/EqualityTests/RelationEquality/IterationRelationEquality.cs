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

using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class IterationRelationEquality
    {
        readonly IterationRelation root;
        readonly IterationRelation clone;
        readonly IterationRelation notEqual;

        public IterationRelationEquality()
        {
            root = new IterationRelation()
            {
                Emit = new List<int>() { 1, 2, 3 },
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
                IterationName = "i1",
                LoopPlan = new ReadRelation()
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
                MaxIterations = 10,
                SkipIterateCondition = new BoolLiteral() { Value = true }
            };
            clone = new IterationRelation()
            {
                Emit = new List<int>() { 1, 2, 3 },
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
                IterationName = "i1",
                LoopPlan = new ReadRelation()
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
                MaxIterations = 10,
                SkipIterateCondition = new BoolLiteral() { Value = true }
            };
            notEqual = new IterationRelation()
            {
                Emit = new List<int>() { 1, 2, 3, 4 },
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c3" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t3" }
                    }
                },
                IterationName = "i2",
                LoopPlan = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c3" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t3" }
                    }
                },
                MaxIterations = 20,
                SkipIterateCondition = new BoolLiteral() { Value = false }
            };
        }

        [Fact]
        public void IsEqual()
        {
            Assert.Equal(root, clone);
        }

        [Fact]
        public void HashCodeIsEqual()
        {
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
        }

        [Fact]
        public void IsNotEqual()
        {
            Assert.NotEqual(root, notEqual);
        }

        [Fact]
        public void InputChangedNotEqual()
        {
            clone.Input = notEqual.Input;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void IterationNameChangedNotEqual()
        {
            clone.IterationName = notEqual.IterationName;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void LoopPlanChangedNotEqual()
        {
            clone.LoopPlan = notEqual.LoopPlan;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void MaxIterationsChangedNotEqual()
        {
            clone.MaxIterations = notEqual.MaxIterations;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void SkipIterateConditionChangedNotEqual()
        {
            clone.SkipIterateCondition = notEqual.SkipIterateCondition;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitChangedNotEqual()
        {
            clone.Emit = notEqual.Emit;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitNullNotEqual()
        {
            clone.Emit = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void InputNullNotEqual()
        {
            clone.Input = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void SkipIterateConditionNullNotEqual()
        {
            clone.SkipIterateCondition = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void MaxIterationsNullNotEqual()
        {
            clone.MaxIterations = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EqualsOperator()
        {
            Assert.True(root == clone);
            Assert.False(root == notEqual);
        }

        [Fact]
        public void NotEqualsOperator()
        {
            Assert.False(root != clone);
            Assert.True(root != notEqual);
        }
    }
}
