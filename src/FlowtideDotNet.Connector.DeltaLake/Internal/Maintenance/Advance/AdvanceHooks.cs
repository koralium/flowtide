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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// The step boundaries of an advance that test hooks hold or fail at.
    /// </summary>
    internal static class AdvanceSteps
    {
        public const string Hint = nameof(Hint);
        public const string Header = nameof(Header);
        public const string Recheck = nameof(Recheck);
        public const string Reserved = nameof(Reserved);
        public const string Started = nameof(Started);
        public const string HandedOff = nameof(HandedOff);
        public const string Resumed = nameof(Resumed);
        public const string LogSlice = nameof(LogSlice);
        public const string LogVerified = nameof(LogVerified);
        public const string LogsComplete = nameof(LogsComplete);
        public const string PayloadSlice = nameof(PayloadSlice);
        public const string Progress = nameof(Progress);
        public const string Done = nameof(Done);
        public const string Teardown = nameof(Teardown);
        public const string Rebase = nameof(Rebase);
    }

    /// <summary>
    /// What tests observe or hold in an advance's steps, deletes and publications; empty in production.
    /// </summary>
    internal sealed class AdvanceHooks
    {
        // Awaited at step boundaries, never inside a slice's progress settlement
        public Func<string, Task>? Step { get; set; }

        // Set on every executor an advance builds
        public Func<DeletionTarget, Task>? BeforeDelete { get; set; }

        public Action<DeletionTarget, DeleteOutcome>? Settled { get; set; }

        // Awaited before each publication attempt, native or object, with the record's kind and name
        public Func<ControlKind, string, Task>? BeforePublish { get; set; }

        public Task StepAsync(string step) => Step?.Invoke(step) ?? Task.CompletedTask;
    }
}
