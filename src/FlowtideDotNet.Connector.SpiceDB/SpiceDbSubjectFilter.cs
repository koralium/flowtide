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

namespace Authzed.Api.V1
{
    internal sealed partial class SubjectFilter : ISpiceDbSubjectFilter
    {
        ISpiceDbRelationFilter ISpiceDbSubjectFilter.OptionalRelation 
        { 
            get => OptionalRelation; 
            set
            {
                if (value is Types.RelationFilter relationFilter)
                {
                    this.OptionalRelation = relationFilter;
                }
                else if (value != null)
                {
                    var rel = (ISpiceDbRelationFilter)new Types.RelationFilter();
                    rel.Relation = value.Relation;
                    this.OptionalRelation = (Types.RelationFilter)rel;
                }
                else
                {
                    this.OptionalRelation = null;
                }
            } 
        }

        public static partial class Types
        {
            internal sealed partial class RelationFilter : ISpiceDbRelationFilter
            {

            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a filter on the sub-relation of a subject reference within a SpiceDB
    /// subject filter.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>SubjectFilter.RelationFilter</c> message.
    /// It is used as <see cref="ISpiceDbSubjectFilter.OptionalRelation"/> to further narrow
    /// a subject filter by a specific sub-relation. Use the static <see cref="Create"/>
    /// factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbRelationFilter
    {
        /// <summary>
        /// Gets or sets the name of the sub-relation to match on the subject
        /// (for example, <c>"member"</c>).
        /// </summary>
        string Relation { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbRelationFilter"/> instance with the specified
        /// sub-relation name.
        /// </summary>
        /// <param name="relation">
        /// The name of the sub-relation to match on the subject (for example, <c>"member"</c>).
        /// If <c>null</c>, the relation will not be set.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbRelationFilter"/> with the specified sub-relation name.
        /// </returns>
        public static ISpiceDbRelationFilter Create(string? relation = default)
        {
            var result = new SubjectFilter.Types.RelationFilter();
            if (relation != null)
            {
                result.Relation = relation;
            }
            return result;
        }
    }

    /// <summary>
    /// Represents a filter on the subject side of a relationship in SpiceDB.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>SubjectFilter</c> message.
    /// <see cref="SubjectType"/> is required; all other properties are optional and impose
    /// no additional constraint when left unset. A subject filter is supplied as
    /// <see cref="ISpiceDbRelationshipFilter.OptionalSubjectFilter"/> to narrow a relationship
    /// query by the type, identifier, or sub-relation of its subject side.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbSubjectFilter
    {
        /// <summary>
        /// Gets or sets the type of the subject to filter on, as defined in the SpiceDB schema
        /// (for example, <c>"user"</c>). This field is required.
        /// </summary>
        string SubjectType { get; set; }

        /// <summary>
        /// Gets or sets an optional subject identifier to match.
        /// </summary>
        /// <remarks>
        /// When not set, subjects of any identifier matching the given type are returned.
        /// Supports the wildcard value <c>"*"</c> to match all subjects of the given type.
        /// </remarks>
        string? OptionalSubjectId { get; set; }

        /// <summary>
        /// Gets or sets an optional sub-relation filter to further narrow results by the
        /// relation held by the subject.
        /// </summary>
        /// <remarks>
        /// When not set, no additional constraint is applied to the sub-relation of the
        /// matched subjects.
        /// </remarks>
        ISpiceDbRelationFilter OptionalRelation { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbSubjectFilter"/> instance with the specified
        /// subject type and optional narrowing criteria.
        /// </summary>
        /// <param name="subjectType">
        /// The type of the subject to filter on, as defined in the SpiceDB schema
        /// (for example, <c>"user"</c>). If <c>null</c>, the subject type will not be set.
        /// </param>
        /// <param name="optionalSubjectId">
        /// The specific subject identifier to match. If <c>null</c>, the subject identifier
        /// filter will not be applied.
        /// </param>
        /// <param name="optionalRelation">
        /// An <see cref="ISpiceDbRelationFilter"/> to further narrow results by the sub-relation
        /// held by the subject. If <c>null</c>, no sub-relation filter will be applied.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbSubjectFilter"/> with the specified subject type and
        /// narrowing criteria.
        /// </returns>
        public static ISpiceDbSubjectFilter Create(
            string? subjectType = default,
            string? optionalSubjectId = default,
            ISpiceDbRelationFilter? optionalRelation = default)
        {
            var result = (ISpiceDbSubjectFilter)new SubjectFilter();
            if (subjectType != null)
            {
                result.SubjectType = subjectType;
            }
            if (optionalSubjectId != null)
            {
                result.OptionalSubjectId = optionalSubjectId;
            }
            if (optionalRelation != null)
            {
                result.OptionalRelation = optionalRelation;
            }
            return result;
        }
    }
}
