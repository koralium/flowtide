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
using FlowtideDotNet.Connector.SpiceDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Authzed.Api.V1
{
    internal sealed partial class SubjectReference : ISpiceDbSubjectReference
    {
        ISpiceDbObjectReference ISpiceDbSubjectReference.Object 
        { 
            get => Object; 
            set
            {
                if (value is ObjectReference reference)
                {
                    this.Object = reference;
                }
                else if (value != null)
                {
                    this.Object = new ObjectReference()
                    {
                        ObjectType = value.ObjectType,
                        ObjectId = value.ObjectId,
                    };
                }
                else
                {
                    this.Object = null;
                }
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a reference to the subject portion of a SpiceDB relationship, identifying
    /// a subject by its object reference and an optional sub-relation.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>SubjectReference</c> message. A subject
    /// reference is used as <see cref="ISpiceDbRelationship.Subject"/> to identify who holds
    /// a relation on a resource. The optional relation component enables referencing a
    /// sub-relation on the subject — for example, <c>group:engineering#members</c> to refer
    /// to the members of a group rather than the group itself.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbSubjectReference
    {
        /// <summary>
        /// Gets or sets the object reference that identifies the subject.
        /// </summary>
        ISpiceDbObjectReference Object { get; set; }

        /// <summary>
        /// Gets or sets an optional sub-relation on the subject that further qualifies who
        /// the subject is (for example, <c>"members"</c> to refer to the members of a group).
        /// </summary>
        /// <remarks>
        /// When not set, the subject is the object identified by <see cref="Object"/> directly.
        /// When set, the subject is the set of objects reachable via the named relation on
        /// the referenced object.
        /// </remarks>
        string OptionalRelation { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbSubjectReference"/> instance with the specified
        /// object reference and optional sub-relation.
        /// </summary>
        /// <param name="obj">
        /// The <see cref="ISpiceDbObjectReference"/> identifying the subject.
        /// If <c>null</c>, the object reference will not be set.
        /// </param>
        /// <param name="optionalRelation">
        /// The name of the sub-relation on the subject (for example, <c>"members"</c>).
        /// If <c>null</c>, no sub-relation will be set and the subject refers to the object
        /// identified by <paramref name="obj"/> directly.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbSubjectReference"/> with the specified object reference
        /// and optional sub-relation.
        /// </returns>
        public static ISpiceDbSubjectReference Create(
            ISpiceDbObjectReference? obj = default,
            string? optionalRelation = default)
        {
            var result = (ISpiceDbSubjectReference)new SubjectReference();
            if (obj != null)
            {
                result.Object = obj;
            }
            if (optionalRelation != null)
            {
                result.OptionalRelation = optionalRelation;
            }
            return result;
        }
    }
}
