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
    internal sealed partial class Relationship : ISpiceDbRelationship
    {
        ISpiceDbObjectReference ISpiceDbRelationship.Resource 
        { 
            get => Resource; 
            set
            {
                if (value is ObjectReference reference)
                {
                    this.Resource = reference;
                }
                else if (value != null)
                {
                    this.Resource = new ObjectReference()
                    {
                        ObjectType = value.ObjectType,
                        ObjectId = value.ObjectId,
                    };
                }
                else
                {
                    this.Resource = null;
                }
            }
        }

        ISpiceDbSubjectReference ISpiceDbRelationship.Subject 
        { 
            get => Subject; 
            set
            {
                if (value is SubjectReference reference)
                {
                    this.Subject = reference;
                }
                else if (value != null)
                {
                    var subjectRef = (ISpiceDbSubjectReference)new SubjectReference();
                    subjectRef.Object = value.Object;
                    subjectRef.OptionalRelation = value.OptionalRelation;
                    this.Subject = (SubjectReference)subjectRef;
                }
                else
                {
                    this.Subject = null;
                }
            }
        }

        ISpiceDbContextualizedCaveat ISpiceDbRelationship.OptionalCaveat 
        { 
            get => OptionalCaveat; 
            set
            {
                if (value is ContextualizedCaveat caveat)
                {
                    this.OptionalCaveat = caveat;
                }
                else if (value != null)
                {
                    var caveatRef = (ISpiceDbContextualizedCaveat)new ContextualizedCaveat();
                    caveatRef.CaveatName = value.CaveatName;
                    caveatRef.Context = value.Context;
                    this.OptionalCaveat = (ContextualizedCaveat)caveatRef;
                }
                else
                {
                    this.OptionalCaveat = null;
                }
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a SpiceDB relationship that specifies how a resource relates to a subject.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>Relationship</c> message. Relationships form
    /// the data for the graph over which all permissions questions are answered. They are carried
    /// as the <see cref="ISpiceDbRelationshipUpdate.Relationship"/> field when writing to SpiceDB
    /// via <see cref="ISpiceDbWriteRelationshipRequest"/>. Use the static <see cref="Create"/>
    /// factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbRelationship
    {
        /// <summary>
        /// Gets or sets the resource object reference to which the subject is related.
        /// </summary>
        ISpiceDbObjectReference Resource { get; set; }

        /// <summary>
        /// Gets or sets the name of the relation that describes how the resource and the subject
        /// are related (for example, <c>"reader"</c> or <c>"writer"</c>).
        /// </summary>
        string Relation { get; set; }

        /// <summary>
        /// Gets or sets the subject reference that holds the relation on the resource.
        /// </summary>
        ISpiceDbSubjectReference Subject { get; set; }

        /// <summary>
        /// Gets or sets an optional caveat that must evaluate to <c>true</c> for this relationship
        /// to be considered active.
        /// </summary>
        /// <remarks>
        /// When <c>null</c>, the relationship is unconditional. When set, the caveat expression
        /// named by <see cref="ISpiceDbContextualizedCaveat.CaveatName"/> is evaluated with the
        /// provided context at permission-check time.
        /// </remarks>
        ISpiceDbContextualizedCaveat OptionalCaveat { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbRelationship"/> instance with the specified resource,
        /// relation, subject, and optional caveat.
        /// </summary>
        /// <param name="resource">
        /// The <see cref="ISpiceDbObjectReference"/> identifying the resource to which the subject
        /// is related. If <c>null</c>, the resource will not be set.
        /// </param>
        /// <param name="relation">
        /// The name of the relation between the resource and the subject (for example,
        /// <c>"reader"</c>). If <c>null</c>, the relation will not be set.
        /// </param>
        /// <param name="subject">
        /// The <see cref="ISpiceDbSubjectReference"/> identifying the subject that holds the
        /// relation on the resource. If <c>null</c>, the subject will not be set.
        /// </param>
        /// <param name="optionalCaveat">
        /// An optional <see cref="ISpiceDbContextualizedCaveat"/> that conditionalizes the
        /// relationship. If <c>null</c>, no caveat will be applied and the relationship is
        /// unconditional.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbRelationship"/> with the specified resource, relation,
        /// subject, and caveat.
        /// </returns>
        public static ISpiceDbRelationship Create(
            ISpiceDbObjectReference? resource = default,
            string? relation = default,
            ISpiceDbSubjectReference? subject = default,
            ISpiceDbContextualizedCaveat? optionalCaveat = default)
        {
            var result = (ISpiceDbRelationship)new Relationship();
            if (resource != null)
            {
                result.Resource = resource;
            }
            if (relation != null)
            {
                result.Relation = relation;
            }
            if (subject != null)
            {
                result.Subject = subject;
            }
            if (optionalCaveat != null)
            {
                result.OptionalCaveat = optionalCaveat;
            }
            return result;
        }
    }
}
