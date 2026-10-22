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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal static class DeltaMetadataCopy
    {
        /// <summary>
        /// A copy of the eight metaData fields that shares no list or dictionary with the source; a retention value replaces both retention keys.
        /// </summary>
        public static DeltaMetadataAction Copy(DeltaMetadataAction source, string? retention)
        {
            var configuration = source.Configuration != null ? new Dictionary<string, string>(source.Configuration) : null;
            if (retention != null)
            {
                configuration ??= new Dictionary<string, string>();
                configuration[RetentionProperties.LogKey] = retention;
                configuration[RetentionProperties.DeletedFileKey] = retention;
            }
            return new DeltaMetadataAction()
            {
                Id = source.Id,
                Name = source.Name,
                Description = source.Description,
                Format = source.Format == null ? null : new DeltaMetadataFormat()
                {
                    Provider = source.Format.Provider,
                    Options = source.Format.Options != null ? new Dictionary<string, string>(source.Format.Options) : null
                },
                // The string as read, never the parsed schema written again
                SchemaString = source.SchemaString,
                PartitionColumns = source.PartitionColumns != null ? new List<string>(source.PartitionColumns) : null,
                CreatedTime = source.CreatedTime,
                Configuration = configuration
            };
        }
    }
}
