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

namespace FlowtideDotNet.Base.Vertices
{
    /// <summary>
    /// Pause gate for a vertex. Pause and Resume are called from stream context gate sweeps
    /// that are not serialized against each other: a user pause racing a resume, a stop or
    /// delete superseding a pause, and the running state re-applying gates on a restart. Both
    /// must therefore be safe to call concurrently and repeatedly - pause keeps an existing
    /// gate instead of assigning a second one (which would orphan the gate an operator already
    /// awaits), and resume takes the gate atomically and completes it with TrySetResult (a
    /// bare SetResult on a gate two sweeps both observed throws, and the exception can escape
    /// the stop path before the stop is dispatched). Mirrors the gate handling in
    /// <c>IngressOutput</c>.
    /// </summary>
    internal sealed class PauseGate
    {
        private readonly object _lock = new object();
        private volatile TaskCompletionSource? _source;

        /// <summary>
        /// True while the gate is closed. A single volatile read so per-message hot paths can
        /// check it cheaply; await through <see cref="PauseTask"/>, which captures the task
        /// safely against a concurrent resume.
        /// </summary>
        public bool IsPaused => _source != null;

        /// <summary>
        /// The task that completes when the gate opens, or null when the gate is not closed.
        /// </summary>
        public Task? PauseTask => _source?.Task;

        public void Pause()
        {
            lock (_lock)
            {
                _source ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public void Resume()
        {
            TaskCompletionSource? gate;
            lock (_lock)
            {
                gate = _source;
                _source = null;
            }
            gate?.TrySetResult();
        }
    }
}
