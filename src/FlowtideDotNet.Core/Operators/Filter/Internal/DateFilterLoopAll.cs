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

using FlowtideDotNet.Core.Compute.Filter;
using FlowtideDotNet.Core.Storage;
using FlexBuffers;
using FlowtideDotNet.Substrait.Relations;

//namespace FlowtideDotNet.Core.Operators.Filter.Internal
//{
//    internal class DateFilterLoopAll : IFilterImplementation
//    {
//        private Func<StreamEvent, FlxValue, bool> _expression;
//        private IStorageTree<StreamEvent> _storageTree;
//        private FlxValue _previousTime;
//        private TimeSpan _interval;
            
//        public DateFilterLoopAll(FilterRelation filterRelation)
//        {
//            _expression = FilterCompiler.CompileWithDate(filterRelation.Condition);
//            var dateIntervalVisitor = new DateIntervalVisitor();
//            dateIntervalVisitor.Visit(filterRelation.Condition, null);
//            if (dateIntervalVisitor.Interval.HasValue)
//            {
//                _interval = dateIntervalVisitor.Interval.Value;
//            }
//            else
//            {
//                _interval = TimeSpan.FromMinutes(1);
//            }
//        }

//        public async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
//        {
//            var currentTime = _previousTime;
//            List<StreamEvent> output = new List<StreamEvent>();
//            foreach (var e in msg.Events)
//            {
//                var weights = _storageTree.UpsertAndGetWeights(e, e.Weight);

//                if (_expression(e, currentTime))
//                {
//                    output.Add(e);
//                }
//            }

//            if (output.Count > 0)
//            {
//                yield return new StreamEventBatch(null, output);
//            }
//        }

//        /// <summary>
//        /// Loops through all the events, and checks which ones no longer hold the time check
//        /// </summary>
//        /// <returns></returns>
//        private async IAsyncEnumerable<StreamEventBatch> CheckTime()
//        {
//            var currentTime = FlxValue.FromBytes(FlexBuffer.SingleValue(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000000));
//            var prevTime = _previousTime;
//            var iterator = _storageTree.CreateIterator();
//            iterator.SeekFirst();

//            List<StreamEvent> output = new List<StreamEvent>();

//            while (iterator.Next())
//            {
//                bool includedBefore = _expression(iterator.CurrentKey, prevTime);
//                bool includedNow = _expression(iterator.CurrentKey, currentTime);

//                // Existed before but not now, should create a delete
//                if (includedBefore && !includedNow)
//                {
//                    output.Add(new StreamEvent(-iterator.CurrentValue, 0, iterator.CurrentKey.Memory));
//                }
//                if (!includedBefore && includedNow)
//                {
//                    output.Add(new StreamEvent(iterator.CurrentValue, 0, iterator.CurrentKey.Memory));
//                }
//            }

//            _previousTime = currentTime;

//            if (output.Count > 0)
//            {
//                yield return new StreamEventBatch(null, output);
//            }
//        }

//        public IAsyncEnumerable<StreamEventBatch> OnTrigger(string triggerName, object? state)
//        {
//            return CheckTime();
//        }

//        public async Task Initialize(string streamName, string operatorName, Func<string, TimeSpan?, Task> addTriggerFunc)
//        {
//            _storageTree = ZoneTreeCreator.CreateTree(streamName, operatorName, "1", 0);
//            await addTriggerFunc(operatorName, TimeSpan.FromSeconds(10));
//        }

//        public Task Compact()
//        {
//            return _storageTree.Compact();
//        }

//        public async Task<object?> OnCheckpoint()
//        {
//            // TODO: Fix state handling
//            await _storageTree.Checkpoint();
//            return null;
//        }

//        public Task InitializeOrRestore(string streamName, string operatorName, Func<string, TimeSpan?, Task> addTriggerFunc, object? state)
//        {
//            //TODO: Take from state
//            _previousTime = FlxValue.FromBytes(FlexBuffer.SingleValue(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000000));
//            _storageTree = ZoneTreeCreator.CreateTree(streamName, operatorName, "1", 0);
//            addTriggerFunc(operatorName, _interval);
//            return Task.CompletedTask;
//        }

//        public Task DeleteAsync()
//        {
//            if (_storageTree == null)
//            {
//                //TODO: Fix
//                //_storageTree = ZoneTreeCreator.CreateTree(streamName, operatorName, "1", 0);
//            }
//            throw new NotImplementedException();
//        }
//    }
//}
