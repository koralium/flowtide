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

using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageVisitorState
    {
        public DirectFieldReference DirectFieldReference { get; }

        public IReadOnlyList<LineageTransformation> Transformations { get; }

        public LineageVisitorState(DirectFieldReference directFieldReference, IReadOnlyList<LineageTransformation> transformations)
        {
            DirectFieldReference = directFieldReference;
            Transformations = transformations;
        }

        public IReadOnlyList<LineageTransformation> AppendTransformation(LineageTransformation lineageTransformation)
        {
            List<LineageTransformation> newTransformations = new List<LineageTransformation>();

            bool containsIdentity = false;
            bool containsTransformation = false;

            foreach (var transformation in Transformations)
            {
                if (transformation.Type == LineageTransformationType.Direct && transformation.SubType == LineageTransformationSubtype.Identity)
                {
                    containsIdentity = true;
                }
                if (transformation.Type == LineageTransformationType.Direct && transformation.SubType == LineageTransformationSubtype.Transformation)
                {
                    containsTransformation = true;
                }
                if (lineageTransformation.Type == LineageTransformationType.Indirect && 
                    transformation.Type == LineageTransformationType.Direct)
                {
                    // Skip direct if it is now indirect.
                    continue;
                }
                if (lineageTransformation.Type == LineageTransformationType.Direct && 
                    (lineageTransformation.SubType == LineageTransformationSubtype.Transformation ||
                    lineageTransformation.SubType == LineageTransformationSubtype.Aggregation))
                {
                    if (transformation.SubType != LineageTransformationSubtype.Identity)
                    {
                        newTransformations.Add(transformation);
                    }
                }
                else
                {
                    newTransformations.Add(transformation);
                }
            }

            if (lineageTransformation.Type == LineageTransformationType.Direct && lineageTransformation.SubType == LineageTransformationSubtype.Identity)
            {
                if (!containsIdentity && !containsTransformation)
                {
                    newTransformations.Add(lineageTransformation);
                }
            }
            else
            {
                newTransformations.Add(lineageTransformation);
            }

            return newTransformations;
        }
    }
}
