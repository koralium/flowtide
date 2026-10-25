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

using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// A storage wrapper the connector itself created; only these are unwrapped to find the raw storage.
    /// </summary>
    internal interface IConnectorOwnedStorage
    {
        IFileStorage Inner { get; }
    }

    internal static class RawStorage
    {
        /// <summary>
        /// The storage below every connector-owned wrapper; foreign wrappers are kept, they are the storage the user configured.
        /// </summary>
        public static IFileStorage Unwrap(IFileStorage storage)
        {
            while (storage is IConnectorOwnedStorage owned)
            {
                storage = owned.Inner;
            }
            return storage;
        }
    }
}
