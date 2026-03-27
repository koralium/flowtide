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

using FlowtideDotNet.Base.Engine;
using System.Diagnostics;

namespace FlowtideDotNet.Engine.FailureStrategies
{
    /// <summary>
    /// An <see cref="IFailureListener"/> that immediately terminates the current process
    /// when the stream encounters an unhandled failure.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Register this strategy via <see cref="DataflowStreamBuilder.AddFailureListener"/> or
    /// <c>FlowtideBuilder.WithFailureListener</c> to force a hard process termination when the
    /// stream fails, bypassing the stream's built-in automatic recovery and any registered
    /// shutdown handlers. This ensures that a container orchestrator such as Kubernetes or Docker
    /// always observes a non-zero process exit, triggering a clean container restart even if
    /// threads are stuck or cleanup code would otherwise hang.
    /// </para>
    /// <para>
    /// <see cref="OnFailure"/> calls <see cref="Process.Kill()"/> on the current process, which
    /// terminates immediately without running <c>AppDomain.ProcessExit</c> event handlers,
    /// object finalizers, or any other cleanup code. For a graceful shutdown that allows
    /// registered cleanup handlers to run before the process exits, use
    /// <see cref="ExitProcessStrategy"/> instead.
    /// </para>
    /// </remarks>
    public class KillProcessStrategy : IFailureListener
    {
        /// <summary>
        /// Immediately terminates the current process by calling <see cref="Process.Kill()"/>
        /// on <see cref="Process.GetCurrentProcess()"/>.
        /// </summary>
        /// <remarks>
        /// This method does not return. The process is terminated immediately without running
        /// <c>AppDomain.ProcessExit</c> event handlers, object finalizers, or any other cleanup code.
        /// The contents of <paramref name="notification"/> are not used by this implementation.
        /// </remarks>
        /// <param name="notification">
        /// The <see cref="StreamFailureNotification"/> describing the stream failure.
        /// Not used by this implementation.
        /// </param>
        public void OnFailure(StreamFailureNotification notification)
        {
            Process.GetCurrentProcess().Kill();
        }
    }
}
