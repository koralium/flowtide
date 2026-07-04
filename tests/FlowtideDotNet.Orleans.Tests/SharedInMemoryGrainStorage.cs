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

using Orleans.Runtime;
using Orleans.Storage;
using System.Collections.Concurrent;
using System.Text.Json;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Grain storage backed by a static dictionary shared by all silos in the test process.
    /// Unlike the Orleans memory grain storage, whose partitions live on storage grains that
    /// die with their silo, this state survives a silo stopping, so tests can stop a silo and
    /// verify that substream grains reactivate on the remaining silo from their stored state.
    /// State is stored as JSON so silos do not share object instances.
    /// </summary>
    internal sealed class SharedInMemoryGrainStorage : IGrainStorage
    {
        private static readonly ConcurrentDictionary<string, (string Json, string ETag)> _store = new ConcurrentDictionary<string, (string, string)>();

        private static string Key<T>(string stateName, GrainId grainId) => $"{stateName}|{grainId}";

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (_store.TryGetValue(Key<T>(stateName, grainId), out var entry))
            {
                grainState.State = JsonSerializer.Deserialize<T>(entry.Json)!;
                grainState.ETag = entry.ETag;
                grainState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }
            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var etag = Guid.NewGuid().ToString();
            _store[Key<T>(stateName, grainId)] = (JsonSerializer.Serialize(grainState.State), etag);
            grainState.ETag = etag;
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _store.TryRemove(Key<T>(stateName, grainId), out _);
            grainState.ETag = null;
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }
    }
}
