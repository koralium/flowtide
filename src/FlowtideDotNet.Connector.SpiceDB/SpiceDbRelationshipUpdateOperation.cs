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

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Specifies the mutation operation to perform on a relationship in a SpiceDB write request.
    /// </summary>
    /// <remarks>
    /// This enum wraps the Authzed API v1 <c>RelationshipUpdate.Operation</c> type. It is set via
    /// <see cref="ISpiceDbRelationshipUpdate.Operation"/> to control how the accompanying
    /// relationship is persisted when the request is committed.
    /// </remarks>
    public enum SpiceDbRelationshipUpdateOperation
    {
        /// <summary>
        /// The operation is unspecified. This value must not be used in a relationship update.
        /// </summary>
        Unspecified = 0,

        /// <summary>
        /// Creates the relationship only if it does not already exist.
        /// The request will fail if the relationship already exists.
        /// </summary>
        Create = 1,

        /// <summary>
        /// Creates the relationship if it does not exist, or updates it if it already exists
        /// (upsert). The request will not fail if the relationship already exists.
        /// </summary>
        Touch = 2,

        /// <summary>
        /// Deletes the relationship. If the relationship does not exist, this operation will no-op.
        /// </summary>
        Delete = 3
    }
}
