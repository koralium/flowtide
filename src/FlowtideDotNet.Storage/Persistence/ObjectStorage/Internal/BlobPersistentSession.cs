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

using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class BlobPersistentSession : IPersistentStorageSession
    {
        private readonly int _maxFileSize;
        private BlobFileWriter _fileWriter;

        /// <summary>
        /// A dictionary that holds the locations of pages before they are flushed to disk.
        /// This allows a reader to still read the data before it is written to disk.
        /// </summary>
        private Dictionary<long, PageWriteLocation> _temporaryWrittenPageLocations;
        private readonly object _lock = new object();
        private readonly BlobPersistentStorage _persistentStorage;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly HashSet<long> _deletedPages;

        public BlobPersistentSession(
            BlobPersistentStorage persistentStorage, 
            IMemoryAllocator memoryAllocator,
            int maxFileSize)
        {
            _maxFileSize = maxFileSize;
            this._persistentStorage = persistentStorage;
            this._memoryAllocator = memoryAllocator;
            _temporaryWrittenPageLocations = new Dictionary<long, PageWriteLocation>();
            _deletedPages = new HashSet<long>();
            SetupFileWriter();
        }

        [MemberNotNull(nameof(_fileWriter))]
        private void SetupFileWriter()
        {
            _fileWriter = new BlobFileWriter(OnFileWritten, MemoryPool<byte>.Shared, _memoryAllocator);
        }

        private void OnFileWritten(PagesFile pagesFile)
        {
            lock (_lock)
            {
                for (int i = 0; i < pagesFile.PageIds.Count; i++)
                {
                    long pageId = pagesFile.PageIds[i];
                    _temporaryWrittenPageLocations.Remove(pageId);
                }
            }
        }

        public async Task Commit()
        {
            if (_fileWriter.PageIds.Count > 0)
            {
                // Send non full file to persistent storage, this allows for writing multiple files in a checkpoint
                // and then merging them together before writing to disk
                await _persistentStorage.AddNonCompletedBlobFile(_fileWriter);
                SetupFileWriter();
            }
            lock (_lock)
            {
                // Send the deleted pages
                if (_deletedPages.Count > 0)
                {
                    _persistentStorage.DeletePages(_deletedPages);
                }
            }
        }

        public Task Delete(long key)
        {
            // Deletes are added to a local set, and will be passed on commit.
            // This is to ensure that only deletes for the current checkpoint are passed.
            lock (_lock)
            {
                // Remove from temporary written since its no longer active
                if (_temporaryWrittenPageLocations.ContainsKey(key))
                {
                    _temporaryWrittenPageLocations.Remove(key);
                }
                
                _deletedPages.Add(key);
            }
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            lock (_lock)
            {
                if (_deletedPages.Contains(key))
                {
                    throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
                }
                if (_temporaryWrittenPageLocations.TryGetValue(key, out var location))
                {
                    return ValueTask.FromResult(stateSerializer.Deserialize(location.data, (int)location.data.Length));
                }
            }

            return _persistentStorage.ReadAsync(key, stateSerializer);
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(long key)
        {
            lock (_lock)
            {
                if (_deletedPages.Contains(key))
                {
                    throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
                }
                if (_temporaryWrittenPageLocations.TryGetValue(key, out var location))
                {
                    //location.data.
                    if (location.data.FirstSpan.Length >= location.data.Length)
                    {
                        var start = location.data.Start;
                        if (location.data.TryGet(ref start, out var value))
                        {
                            return ValueTask.FromResult(value);
                        }
                    }
                    // If the data is not in the first span, we need to copy it to a new buffer
                    var buffer = new byte[location.data.Length];
                    location.data.CopyTo(buffer);
                    return ValueTask.FromResult((ReadOnlyMemory<byte>)buffer);
                }
            }

            // Fetch from storage if not found in temporary locations
            return Read_Slow(key);
        }

        private async ValueTask<ReadOnlyMemory<byte>> Read_Slow(long key)
        {
            var result = await _persistentStorage.ReadAsync(key);

            if (result.IsSingleSegment)
            {
                var start = result.Start;
                if (result.TryGet(ref start, out var value))
                {
                    return value;
                }
            }

            var buffer = new byte[result.Length];
            result.CopyTo(buffer);
            return buffer;
        }

        public async Task Write(long key, SerializableObject value)
        {
            lock (_lock)
            {
                var sequence = _fileWriter.Write(key, value);
                // If the page is in deleted pages, remove it from the set
                // Since it has been written again
                if (_deletedPages.Contains(key))
                {
                    _deletedPages.Remove(key);
                }
                if (_temporaryWrittenPageLocations.ContainsKey(key))
                {
                    throw new FlowtidePersistentStorageException($"Key '{key}' has already been written.");
                }
                // Add info to lookup so reads can find the written data before its flushed to storage
                _temporaryWrittenPageLocations.Add(key, new PageWriteLocation()
                {
                    data = sequence
                });   
            }

            if (_fileWriter.WrittenLength >= _maxFileSize)
            {
                // Finish the file writer, this adds the page ids and offsets
                _fileWriter.Finish();

                // TODO: Send the file to the blob writer
                await _persistentStorage.AddCompleteBlobFile(_fileWriter);

                SetupFileWriter();
            }
        }

        public void Reset()
        {
            lock (_lock)
            {
                _temporaryWrittenPageLocations.Clear();
                _deletedPages.Clear();
                _fileWriter.Dispose();
                SetupFileWriter();
            }
        }
    }
}
