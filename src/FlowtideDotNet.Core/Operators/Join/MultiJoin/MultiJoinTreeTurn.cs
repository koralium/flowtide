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

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// The queue of one input's trees in a round: the batches of the round use them one at a time, in the order of their places.
    /// </summary>
    internal sealed class MultiJoinTreeTurn
    {
        private readonly TaskCompletionSource?[] _waiting;
        private readonly bool[] _released;
        private int _front;

        public MultiJoinTreeTurn(int places)
        {
            _waiting = new TaskCompletionSource?[places];
            _released = new bool[places];
        }

        /// <summary>
        /// Completes when every place ahead has let go. The place holds the trees until it lets go itself.
        /// </summary>
        public Task Enter(int place)
        {
            lock (_released)
            {
                if (_released[place])
                {
                    // Another batch may have stored rows since, reading now would join them twice
                    throw new InvalidOperationException("A batch of a multi join round came back to trees it had let go of.");
                }
                if (_front == place)
                {
                    return Task.CompletedTask;
                }
                // A waiter is resumed on another thread, the one that lets go carries on with its own work
                var waiting = _waiting[place] ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return waiting.Task;
            }
        }

        /// <summary>
        /// Lets go of the trees for good, also for a place that never came to them. Calling it again does nothing.
        /// </summary>
        public void Release(int place)
        {
            TaskCompletionSource? next = null;
            lock (_released)
            {
                if (_released[place])
                {
                    return;
                }
                _released[place] = true;
                while (_front < _released.Length && _released[_front])
                {
                    _front++;
                }
                if (_front < _released.Length)
                {
                    next = _waiting[_front];
                }
            }
            next?.TrySetResult();
        }
    }
}
