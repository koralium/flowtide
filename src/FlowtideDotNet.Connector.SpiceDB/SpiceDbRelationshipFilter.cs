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
    internal sealed partial class RelationshipFilter : ISpiceDbRelationshipFilter
    {
        ISpiceDbSubjectFilter ISpiceDbRelationshipFilter.OptionalSubjectFilter 
        { 
            get => OptionalSubjectFilter;
            set
            {
                if (value is SubjectFilter subjectFilter)
                {
                    this.OptionalSubjectFilter = subjectFilter;
                }
                else if (value != null)
                {
                    var rel = (ISpiceDbSubjectFilter)new SubjectFilter();
                    rel.OptionalSubjectId = value.OptionalSubjectId;
                    rel.OptionalRelation = value.OptionalRelation;
                    rel.SubjectType = value.SubjectType;
                    this.OptionalSubjectFilter = (SubjectFilter)rel;
                }
                else
                {
                    this.OptionalSubjectFilter = null;
                }
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a collection of filters that, when applied to a set of relationships,
    /// return only those with exactly matching fields.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>RelationshipFilter</c> message.
    /// <see cref="ResourceType"/> is required; all other properties are optional and impose
    /// no additional constraint when left unset. A filter is supplied as
    /// <see cref="ISpiceDbReadRelationshipsRequest.RelationshipFilter"/> to scope a read
    /// operation, and as <see cref="ISpiceDbPrecondition.Filter"/> to define the relationship
    /// set evaluated by a precondition. Use the static <see cref="Create"/> factory method to
    /// construct a new instance.
    /// </remarks>
    public interface ISpiceDbRelationshipFilter
    {
        /// <summary>
        /// Gets or sets the type of the resource to filter on, as defined in the SpiceDB schema
        /// (for example, <c>"document"</c>). This field is required.
        /// </summary>
        string ResourceType { get; set; }

        /// <summary>
        /// Gets or sets an optional resource identifier to match.
        /// </summary>
        /// <remarks>
        /// When not set, relationships for any identifier of the given resource type are returned.
        /// </remarks>
        string OptionalResourceId { get; set; }

        /// <summary>
        /// Gets or sets an optional relation name to match (for example, <c>"reader"</c>).
        /// </summary>
        /// <remarks>
        /// When not set, relationships for any relation on the resource type are returned.
        /// </remarks>
        string? OptionalRelation { get; set; }

        /// <summary>
        /// Gets or sets an optional subject filter to further narrow results by subject type,
        /// identifier, or sub-relation.
        /// </summary>
        /// <remarks>
        /// When not set, no additional constraint is applied to the subject side of the
        /// matched relationships.
        /// </remarks>
        ISpiceDbSubjectFilter OptionalSubjectFilter { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbRelationshipFilter"/> instance with the specified
        /// resource type and optional narrowing criteria.
        /// </summary>
        /// <param name="resourceType">
        /// The type of the resource to filter on, as defined in the SpiceDB schema
        /// (for example, <c>"document"</c>). If <c>null</c>, the resource type will not be set.
        /// </param>
        /// <param name="optionalResourceId">
        /// The specific resource identifier to match. If <c>null</c>, the resource identifier
        /// filter will not be applied.
        /// </param>
        /// <param name="optionalRelation">
        /// The relation name to match (for example, <c>"reader"</c>). If <c>null</c>, the
        /// relation filter will not be applied.
        /// </param>
        /// <param name="optionalSubjectFilter">
        /// An <see cref="ISpiceDbSubjectFilter"/> to further narrow results by subject type,
        /// identifier, or sub-relation. If <c>null</c>, no subject filter will be applied.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbRelationshipFilter"/> with the specified resource type
        /// and narrowing criteria.
        /// </returns>
        public static ISpiceDbRelationshipFilter Create(
            string? resourceType = default,
            string? optionalResourceId = default,
            string? optionalRelation = default,
            ISpiceDbSubjectFilter? optionalSubjectFilter = default)
        {
            var result = (ISpiceDbRelationshipFilter)new RelationshipFilter();
            if (resourceType != null)
            {
                result.ResourceType = resourceType;
            }
            if (optionalResourceId != null)
            {
                result.OptionalResourceId = optionalResourceId;
            }
            if (optionalRelation != null)
            {
                result.OptionalRelation = optionalRelation;
            }
            if (optionalSubjectFilter != null)
            {
                result.OptionalSubjectFilter = optionalSubjectFilter;
            }
            return result;
        }
    }
}
