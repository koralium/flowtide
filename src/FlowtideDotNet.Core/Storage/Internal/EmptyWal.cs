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

//namespace DifferentialCompute.Core.Storage.Internal
//{
//    internal class EmptyWal<TKey, TValue> : IWriteAheadLog<TKey, TValue>, CheckWal
//    {
//        public string FilePath => throw new NotImplementedException();

//        public bool EnableIncrementalBackup { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

//        public Task Completed => Task.CompletedTask;

//        public void Append(in TKey key, in TValue value, long opIndex)
//        {
//            throw new NotImplementedException();
//        }

//        public void Dispose()
//        {
//        }

//        public void Drop()
//        {
//        }

//        public void MarkFrozen()
//        {
//        }

//        public WriteAheadLogReadLogEntriesResult<TKey, TValue> ReadLogEntries(bool stopReadOnException, bool stopReadOnChecksumFailure, bool sortByOpIndexes)
//        {
//            return new WriteAheadLogReadLogEntriesResult<TKey, TValue>()
//            {
//                Keys = new List<TKey>(),
//                Success = true,
//                Values = new List<TValue>()
//            };
//        }

//        public long ReplaceWriteAheadLog(TKey[] keys, TValue[] values, bool disableBackup)
//        {
//            throw new NotImplementedException();
//        }

//        public void TruncateIncompleteTailRecord(IncompleteTailRecordFoundException incompleteTailException)
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
