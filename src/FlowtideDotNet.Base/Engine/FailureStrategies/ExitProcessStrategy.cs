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

namespace FlowtideDotNet.Engine.FailureStrategies
{
    /// <summary>
    /// An <see cref="IFailureListener"/> that terminates the current process gracefully
    /// when the stream encounters an unhandled failure.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Register this strategy via <see cref="DataflowStreamBuilder.AddFailureListener"/> or
    /// <c>FlowtideBuilder.WithFailureListener</c> to let a container orchestrator such as
    /// Kubernetes or Docker restart the process when the stream fails, rather than allowing
    /// the stream to attempt its built-in automatic recovery.
    /// </para>
    /// <para>
    /// <see cref="OnFailure"/> calls <see cref="Environment.Exit"/> with the current
    /// <see cref="Environment.ExitCode"/>, which initiates a graceful shutdown: registered
    /// <c>AppDomain.ProcessExit</c> handlers and object finalizers are executed before the
    /// process terminates. For an immediate, non-graceful termination that skips cleanup,
    /// use <see cref="KillProcessStrategy"/> instead.
    /// </para>
    /// </remarks>
    public class ExitProcessStrategy : IFailureListener
    {
        /// <summary>
        /// Terminates the current process gracefully by calling
        /// <see cref="Environment.Exit"/> with <see cref="Environment.ExitCode"/>.
        /// </summary>
        /// <remarks>
        /// This method does not return. The process exits after running any registered
        /// <c>AppDomain.ProcessExit</c> event handlers and object finalizers.
        /// The contents of <paramref name="notification"/> are not used by this implementation.
        /// </remarks>
        /// <param name="notification">
        /// The <see cref="StreamFailureNotification"/> describing the stream failure.
        /// Not used by this implementation.
        /// </param>
        public void OnFailure(StreamFailureNotification notification)
        {
            Environment.Exit(Environment.ExitCode);
        }
    }
}
