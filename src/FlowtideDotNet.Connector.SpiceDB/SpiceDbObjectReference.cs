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
    internal sealed partial class ObjectReference : ISpiceDbObjectReference
    {

    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a reference to a specific object in the SpiceDB permission system,
    /// identified by its type and unique identifier.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>ObjectReference</c> message.
    /// Object references identify the resource side of a relationship via
    /// <see cref="ISpiceDbRelationship.Resource"/>, and form the object component
    /// of a subject reference via <see cref="ISpiceDbSubjectReference.Object"/>.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbObjectReference
    {
        /// <summary>
        /// Gets or sets the type of the object, as defined in the SpiceDB schema
        /// (for example, <c>"document"</c> or <c>"user"</c>).
        /// </summary>
        string ObjectType { get; set; }

        /// <summary>
        /// Gets or sets the unique identifier of the object within its type.
        /// </summary>
        /// <remarks>
        /// Supports the wildcard value <c>"*"</c> to match all objects of the given type
        /// when used in relationship filters.
        /// </remarks>
        string ObjectId { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbObjectReference"/> instance with the specified
        /// object type and identifier.
        /// </summary>
        /// <param name="objectType">
        /// The type of the object as defined in the SpiceDB schema (for example, <c>"document"</c>).
        /// If <c>null</c>, the object type will not be set.
        /// </param>
        /// <param name="objectId">
        /// The unique identifier of the object within its type.
        /// If <c>null</c>, the object identifier will not be set.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbObjectReference"/> with the specified object type and identifier.
        /// </returns>
        public static ISpiceDbObjectReference Create(
            string? objectType = default,
            string? objectId = default)
        {
            var result = new ObjectReference();
            if (objectType != null)
            {
                result.ObjectType = objectType;
            }
            if (objectId != null)
            {
                result.ObjectId = objectId;
            }
            return result;
        }
    }
}
