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
    [CollectionDefinition(Name, DisableParallelization = true)]
    public class EgressVertexFaultDisposeCollection
    {
        // Two spinning threads starve the pool, other tests must not run beside them.
        public const string Name = "EgressVertexFaultDispose";
    }

    [Collection(EgressVertexFaultDisposeCollection.Name)]
    public class EgressVertexFaultDisposeTests
    {
        private sealed class TestEgressVertex() : EgressVertex<string>(new ExecutionDataflowBlockOptions())
        {
            public override string DisplayName => "test";
            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        // A linked upstream's PropagateCompletion can call Fault while teardown disposes the vertex.
        [Fact]
        public async Task FaultRacingDisposeNeverThrows()
        {
            const int iterations = 5_000;
            TestEgressVertex? shared = null;
            int go = -1, done = -1, thrown = 0;
            Exception? unexpected = null;

            void WaitFor(ref int step, int i)
            {
                var spin = new SpinWait();
                while (Volatile.Read(ref step) != i && Volatile.Read(ref unexpected) == null)
                {
                    spin.SpinOnce(sleep1Threshold: -1);
                }
            }

            // The offset sweeps which side reaches the token source first.
            void Owner()
            {
                try
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        var vertex = new TestEgressVertex();
                        vertex.CreateBlock();
                        Volatile.Write(ref shared, vertex);
                        Volatile.Write(ref go, i);
                        Thread.SpinWait(Math.Max(0, i % 33 - 16));
                        vertex.DisposeAsync().GetAwaiter().GetResult();
                        WaitFor(ref done, i);
                    }
                }
                catch (Exception e) { Volatile.Write(ref unexpected, e); }
            }

            void Upstream()
            {
                try
                {
                    for (int i = 0; i < iterations; i++)
                    {
                        WaitFor(ref go, i);
                        var vertex = Volatile.Read(ref shared)!;
                        Thread.SpinWait(Math.Max(0, 16 - i % 33));
                        try { vertex.Fault(new InvalidOperationException("upstream failed")); }
                        catch (ObjectDisposedException) { thrown++; }
                        Volatile.Write(ref done, i);
                    }
                }
                catch (Exception e) { Volatile.Write(ref unexpected, e); }
            }

            await Task.WhenAll(Task.Factory.StartNew(Owner, TaskCreationOptions.LongRunning), Task.Factory.StartNew(Upstream, TaskCreationOptions.LongRunning));

            Assert.Null(unexpected);
            Assert.True(thrown == 0, $"Fault threw ObjectDisposedException in {thrown}/{iterations} races with DisposeAsync.");
        }
    }
}
