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
    internal sealed partial class Precondition : ISpiceDbPrecondition
    {
        SpiceDbPreconditionOperation ISpiceDbPrecondition.Operation 
        {
            get => (SpiceDbPreconditionOperation)this.Operation;
            set 
            {
                this.Operation = (Types.Operation)value;
            } 
        }

        ISpiceDbRelationshipFilter ISpiceDbPrecondition.Filter 
        { 
            get => Filter;
            set
            {
                if (value is RelationshipFilter relationshipFilter)
                {
                    this.Filter = relationshipFilter;
                }
                else if (value != null)
                {
                    var rel = (ISpiceDbRelationshipFilter)new RelationshipFilter();
                    rel.OptionalSubjectFilter = value.OptionalSubjectFilter;
                    rel.OptionalRelation = value.OptionalRelation;
                    rel.OptionalResourceId = value.OptionalResourceId;
                    rel.ResourceType = value.ResourceType;
                    this.Filter = (RelationshipFilter)rel;
                }
                else
                {
                    this.Filter = null;
                }
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Specifies how the existence or absence of relationships matching a filter
    /// determines whether a SpiceDB write operation proceeds.
    /// </summary>
    public enum SpiceDbPreconditionOperation
    {
        /// <summary>
        /// The operation is unspecified. This value must not be used in a precondition.
        /// </summary>
        Unspecified = 0,

        /// <summary>
        /// The parent request will fail if any relationships match the associated filter.
        /// </summary>
        MustNotMatch = 1,

        /// <summary>
        /// The parent request will fail if no relationships match the associated filter.
        /// </summary>
        MustMatch = 2
    }

    /// <summary>
    /// Represents a precondition that must be satisfied before a SpiceDB write operation is committed.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>Precondition</c> message. Preconditions assert
    /// the existence or absence of relationships that match a given filter; if any precondition is
    /// not satisfied, the parent write is not committed. Preconditions are supplied via
    /// <see cref="ISpiceDbWriteRelationshipRequest.OptionalPreconditions"/>.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbPrecondition
    {
        /// <summary>
        /// Gets or sets the operation that determines how the filter match result affects
        /// whether the parent request is allowed to proceed.
        /// </summary>
        SpiceDbPreconditionOperation Operation { get; set; }

        /// <summary>
        /// Gets or sets the relationship filter used to evaluate this precondition.
        /// </summary>
        ISpiceDbRelationshipFilter Filter { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbPrecondition"/> instance with the specified
        /// operation and relationship filter.
        /// </summary>
        /// <param name="operation">
        /// The <see cref="SpiceDbPreconditionOperation"/> that determines how the filter result
        /// affects the parent request. If <c>null</c>, the operation will not be set.
        /// </param>
        /// <param name="filter">
        /// The <see cref="ISpiceDbRelationshipFilter"/> used to evaluate this precondition.
        /// If <c>null</c>, the filter will not be set.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbPrecondition"/> with the specified operation and filter.
        /// </returns>
        public static ISpiceDbPrecondition Create(
            SpiceDbPreconditionOperation? operation = default,
            ISpiceDbRelationshipFilter? filter = default)
        {
            var result = (ISpiceDbPrecondition)new Precondition();
            if (operation != null)
            {
                result.Operation = operation.Value;
            }
            if (filter != null)
            {
                result.Filter = filter;
            }
            return result;
        }
    }
}
