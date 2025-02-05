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

using FlowtideDotNet.Storage.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.StateManager.Internal.ObjectState
{
    internal class ObjectStateClient<T> : StateClient, IObjectState<T>
    {
        private StateClientMetadata<T> _metadata;
        private readonly IPersistentStorageSession _session;

        /// <summary>
        /// The previous state, used to determine if the state has changed.
        /// This is saved to reduce the amount of writes to the persistent storage.
        /// </summary>
        private byte[] _previousState;

        public ObjectStateClient(long metadataId, StateClientMetadata<T> metadata, IPersistentStorageSession session)
        {
            MetadataId = metadataId;
            this._metadata = metadata;
            _session = session;
            _previousState = GetSerializedState();
        }

        private byte[] GetSerializedState()
        {
            return StateClientMetadataSerializer.Serialize(_metadata);
        }

        public override long MetadataId { get; }

        public T? Value
        {
            get
            {
                return _metadata.Metadata;
            }
            set
            {
                _metadata.Metadata = value;
            }
        }

        public async ValueTask Commit()
        {
            if (!_metadata.CommitedOnce)
            {
                _metadata.CommitedOnce = true;
                var newState = GetSerializedState();
                await _session.Write(MetadataId, newState);
                _previousState = newState;
                await _session.Commit();
            }
            else
            {
                var newState = GetSerializedState();
                if (!newState.SequenceEqual(_previousState))
                {
                    await _session.Write(MetadataId, newState);
                    _previousState = newState;
                    await _session.Commit();
                }
            }
        }

        public override void Dispose()
        {
        }

        public override async ValueTask Reset(bool clearMetadata)
        {
            if (clearMetadata)
            {
                _metadata.Metadata = default;
            }
            else
            {
                if (_metadata.CommitedOnce)
                {
                    var bytes = await _session.Read(MetadataId);
                    if (bytes != null)
                    {
                        var result = StateClientMetadataSerializer.Deserialize<T>(new ByteMemoryOwner(bytes), bytes.Length);
                        if (result != null)
                        {
                            _metadata = result;
                        }
                        else
                        {
                            throw new InvalidOperationException("Could not deserialize object from persistent storage");
                        }
                    }
                    else
                    {
                        _metadata.Metadata = default;
                    }
                }
                else
                {
                    _metadata.Metadata = default;
                }
            }
        }
    }
}
