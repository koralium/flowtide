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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using System.Collections.Concurrent;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("StreamContext test hooks")]
    public class BlockInitializeFailureRequestTests : FlowtideAcceptanceBase
    {
        private const string Token = "BlockInitializeFailureRequestTests";

        private readonly ITestOutputHelper _output;

        public BlockInitializeFailureRequestTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
            _output = testOutputHelper;
        }

        /// <summary>
        /// Control: an Initialize that throws gets OnFailure from the failure teardown before the restart.
        /// </summary>
        [Fact]
        public async Task ThrowingInitializeGetsOnFailureBeforeTheRestart()
        {
            var name = $"{Token}_{nameof(ThrowingInitializeGetsOnFailureBeforeTheRestart)}";
            var events = new ConcurrentQueue<string>();
            var attempts = 0;
            await using var stream = new FlowtideTestStream(name)
            {
                FailSourceInitializeWhen = () =>
                {
                    var attempt = Interlocked.Increment(ref attempts);
                    events.Enqueue($"init{attempt}");
                    return attempt == 1;
                },
                SourceOnFailure = version => events.Enqueue($"onfailure({version})")
            };
            await RunAndAssertOnFailureBeforeRestart(stream, name, events, () => Volatile.Read(ref attempts));
        }

        /// <summary>
        /// An Initialize that requests a rollback and then returns normally must still get OnFailure before the restart.
        /// </summary>
        [Fact]
        public async Task RollbackRequestingInitializeGetsOnFailureBeforeTheRestart()
        {
            var name = $"{Token}_{nameof(RollbackRequestingInitializeGetsOnFailureBeforeTheRestart)}";
            var events = new ConcurrentQueue<string>();
            var attempts = 0;
            await using var stream = new FlowtideTestStream(name)
            {
                RollbackSourceInitializeWhen = () =>
                {
                    var attempt = Interlocked.Increment(ref attempts);
                    events.Enqueue($"init{attempt}");
                    return attempt == 1;
                },
                SourceOnFailure = version => events.Enqueue($"onfailure({version})")
            };
            await RunAndAssertOnFailureBeforeRestart(stream, name, events, () => Volatile.Read(ref attempts));
        }

        private async Task RunAndAssertOnFailureBeforeRestart(FlowtideTestStream stream, string name, ConcurrentQueue<string> events, Func<int> attempts)
        {
            var logs = new RingBufferLoggerProvider();
            stream.AddLoggerProvider(logs);
            try
            {
                StreamContext.BeforeFailureDisposeForTests = streamName =>
                {
                    if (streamName.Contains(name)) events.Enqueue("teardown");
                };
                StreamContext.RestoreVersionForTests = (streamName, version) =>
                {
                    if (streamName.Contains(name)) events.Enqueue($"teardown-claimed-blocks({version})");
                };
                stream.Generate(10);

                var start = stream.StartStream("INSERT INTO output SELECT userkey FROM users");
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (attempts() < 2 && !start.IsFaulted)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"The stream never restarted, events: {string.Join(", ", events)}");
                    await Task.Delay(10);
                }
                if (start.IsFaulted)
                {
                    await start;
                }
                await start.WaitAsync(TimeSpan.FromSeconds(30));
                // WaitForUpdate rethrows the injected failure, the restarted run is awaited by state.
                while (stream.State != StreamStateValue.Running)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"The restarted run never reached Running, state {stream.State}, events: {string.Join(", ", events)}");
                    await Task.Delay(10);
                }
            }
            finally
            {
                StreamContext.BeforeFailureDisposeForTests = null;
                StreamContext.RestoreVersionForTests = null;
                var order = string.Join(", ", events);
                _output.WriteLine($"Events: {order}");
                foreach (var line in logs.LinesContaining("abandoning").Concat(logs.LinesContaining("skipping block teardown")).Concat(logs.LinesContaining("Failure handling calling on failure")))
                {
                    _output.WriteLine(line);
                }
            }

            var sequence = events.ToList();
            var described = string.Join(", ", sequence);
            var restartIndex = sequence.IndexOf("init2");
            Assert.True(restartIndex > 0, $"The stream never restarted, events: {described}");
            var failedRun = sequence.Take(restartIndex).ToList();

            // The failure teardown claims the failed run's blocks once and calls OnFailure once.
            var claims = failedRun.Where(e => e.StartsWith("teardown-claimed-blocks")).ToList();
            var onFailures = failedRun.Where(e => e.StartsWith("onfailure")).ToList();
            Assert.True(claims.Count == 1, $"The failure teardown did not claim the failed run's blocks exactly once before the restart, events: {described}");
            Assert.True(onFailures.Count == 1, $"The failed run's source did not get OnFailure exactly once before the restart, events: {described}");

            // With the restore version that teardown decided, after it claimed the blocks.
            var decidedVersion = claims[0].Substring("teardown-claimed-blocks(".Length).TrimEnd(')');
            Assert.Equal($"onfailure({decidedVersion})", onFailures[0]);
            Assert.True(failedRun.IndexOf(claims[0]) < failedRun.IndexOf(onFailures[0]), $"OnFailure ran before the teardown claimed the blocks, events: {described}");

            // The restarted run neither fails nor sees a late OnFailure of the failed run.
            Assert.True(!sequence.Skip(restartIndex).Any(e => e.StartsWith("onfailure") || e == "teardown"), $"OnFailure or a teardown ran after the restart, events: {described}");
        }
    }
}
