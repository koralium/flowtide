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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    /// <summary>
    /// The stream context applies and undoes the vertex pause gates from sweeps that are
    /// not serialized against each other: a user Pause racing a Resume, a stop or delete
    /// superseding a pause, and the running state re-applying gates on a restart. Pause
    /// and Resume on a vertex must therefore be safe to call concurrently. An unlocked
    /// check-then-SetResult lets two concurrent releases observe the same gate and
    /// complete it twice, which throws - and when the loser is the Resume inside
    /// StopAsync, the exception escapes before the stop is dispatched and every later
    /// stop awaits a stop task that never completes. Two concurrent applies can likewise
    /// both assign a gate, orphaning the one an operator already awaits.
    /// </summary>
    public class VertexPauseGateTests
    {
        private sealed class TestEgressVertex : EgressVertex<string>
        {
            public TestEgressVertex() : base(new ExecutionDataflowBlockOptions())
            {
            }

            public override string DisplayName => "test";

            public ValueTask CheckPause() => CheckForPause();

            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        [Fact]
        public async Task ConcurrentPauseAndResumeSweepsNeverThrow()
        {
            var vertex = new TestEgressVertex();

            const int iterations = 50_000;
            using var barrier = new Barrier(2);
            Exception? failure = null;

            void Sweep()
            {
                try
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        barrier.SignalAndWait();
                        vertex.Pause();
                        vertex.Resume();
                    }
                }
                catch (Exception e)
                {
                    Volatile.Write(ref failure, e);
                    // Unblocks the other participant so the test does not hang on the barrier.
                    barrier.RemoveParticipant();
                }
            }

            var first = Task.Run(Sweep);
            var second = Task.Run(Sweep);
            await Task.WhenAll(first, second);

            Assert.True(failure == null, $"Concurrent pause gate sweeps threw: {Volatile.Read(ref failure)}");

            // After the storm the vertex must end in a releasable state: one final resume
            // opens the gate for any operator that captured it.
            vertex.Resume();
            Assert.True(vertex.CheckPause().IsCompleted, "The vertex stayed paused after every pause was resumed - a gate was orphaned by concurrent applies");
        }
    }
}
