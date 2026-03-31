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

using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageInputField : IEquatable<LineageInputField>
    {
        public string Namespace { get; }

        public string TableName { get; }

        public string Field { get; }

        public IReadOnlyList<LineageTransformation> Transformations { get; }

        public LineageInputField(string @namespace, string tableName, string field, IReadOnlyList<LineageTransformation> transformations)
        {
            Namespace = @namespace;
            TableName = tableName;
            Field = field;
            Transformations = transformations;
        }

        public override bool Equals(object? obj)
        {
            return obj is LineageInputField other && Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Namespace, TableName, Field);
        }

        private bool TransformationsEqual(IReadOnlyList<LineageTransformation> transformations1, IReadOnlyList<LineageTransformation> transformations2)
        {
            if (transformations1.Count != transformations2.Count)
            {
                return false;
            }
            for (int i = 0; i < transformations1.Count; i++)
            {
                if (!transformations1[i].Equals(transformations2[i]))
                {
                    return false;
                }
            }
            return true;
        }

        public bool Equals(LineageInputField? other)
        {
            return other != null &&
                Namespace == other.Namespace &&
                TableName == other.TableName &&
                Field == other.Field && 
                TransformationsEqual(Transformations, other.Transformations);
        }
    }
}
