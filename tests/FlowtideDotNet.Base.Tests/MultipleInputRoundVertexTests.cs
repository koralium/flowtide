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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Metrics.Internal;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    public class MultipleInputRoundVertexTests : IAsyncLifetime
    {
        private readonly List<IStreamVertex> _vertices = new List<IStreamVertex>();

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var vertex in _vertices)
            {
                await vertex.DisposeAsync();
            }
        }

        private sealed class StubVertexHandler : IVertexHandler
        {
            public string OperatorId => "op";
            // A name of its own: a build that writes the events of a vertex to a file names the file by it
            public string StreamName { get; } = $"stream-{Guid.NewGuid():N}";
            public IMeter Metrics { get; } = new FlowtideMeter(new Meter("test"), new TagList(), () => "test");
            public IStateManagerClient StateClient => null!;
            public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
            public IOperatorMemoryManager MemoryManager => null!;
            public void ScheduleCheckpoint(TimeSpan time, long? checkpointVersion) { }
            public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null) => Task.CompletedTask;
            public Task FailAndRollback(Exception? exception, long? restoreVersion = default) => Task.CompletedTask;
        }

        private sealed class Payload : IRentable
        {
            public Payload(string name)
            {
                Name = name;
            }

            public string Name { get; }

            public int Rented;
            public int Returned;
            public Action? Returning;

            public void Rent(int count) => Interlocked.Add(ref Rented, count);

            public void Return()
            {
                Returning?.Invoke();
                Interlocked.Increment(ref Returned);
            }
        }

        // Records every call as one line: a message as "0:a", a round as its messages joined by a comma, a trigger as "trigger"
        private class RecordingVertex : MultipleInputRoundVertex<Payload>
        {
            private readonly int _maxMessages;
            private readonly long _maxCost;
            public readonly List<string> Calls = new List<string>();
            public readonly TaskCompletionSource Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public string? FailOn;

            public RecordingVertex(int maxMessages, long maxCost = long.MaxValue, int boundedCapacity = 100) : base(2, new ExecutionDataflowBlockOptions { BoundedCapacity = boundedCapacity })
            {
                _maxMessages = maxMessages;
                _maxCost = maxCost;
            }

            public override string DisplayName => "test";

            public int Queued => QueuedEvents;

            protected override int MaxRoundMessages => _maxMessages;

            protected override long MaxRoundCostPerInput => _maxCost;

            protected override long RoundCost(Payload message) => message.Name.Length;

            // The first call waits, so what the test sends after it is queued together
            private async Task Enter(string call)
            {
                lock (Calls)
                {
                    Calls.Add(call);
                }
                if (!Entered.Task.IsCompleted)
                {
                    Entered.TrySetResult();
                    await Gate.Task;
                }
                if (FailOn != null && call.Contains(FailOn))
                {
                    throw new InvalidOperationException("Injected failure in " + call);
                }
            }

            public override async IAsyncEnumerable<Payload> OnRecieve(int targetId, Payload msg, long time)
            {
                await Enter($"{targetId}:{msg.Name}");
                yield return new Payload("out-" + msg.Name);
            }

            public override async IAsyncEnumerable<Payload> OnTrigger(string name, object? state)
            {
                await Enter("trigger");
                yield break;
            }

            public override Task OnCheckpoint() => Task.CompletedTask;
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class RoundVertex : RecordingVertex
        {
            public RoundVertex(int maxMessages, long maxCost = long.MaxValue, int boundedCapacity = 100) : base(maxMessages, maxCost, boundedCapacity)
            {
            }

            public override async IAsyncEnumerable<Payload> OnRecieveRound(IReadOnlyList<RoundMessage<Payload>> messages, long time)
            {
                lock (Calls)
                {
                    Calls.Add(string.Join(",", messages.Select(x => $"{x.TargetId}:{x.Message.Name}")));
                }
                if (FailOn != null && messages.Any(x => x.Message.Name == FailOn))
                {
                    throw new InvalidOperationException("Injected failure in a round");
                }
                foreach (var message in messages)
                {
                    yield return new Payload("out-" + message.Message.Name);
                }
                await Task.CompletedTask;
            }
        }

        private async Task<BufferBlock<IStreamEvent>> SetupVertex(RecordingVertex vertex)
        {
            var collector = new BufferBlock<IStreamEvent>();
            var handler = new StubVertexHandler();
            _vertices.Add(vertex);
            vertex.Setup(handler.StreamName, "op");
            vertex.LinkTo(collector, new DataflowLinkOptions());
            vertex.CreateBlock();
            vertex.Link();
            await vertex.Initialize("op", 0, 0, handler, null);
            return collector;
        }

        private static async Task<IStreamEvent> ReceiveWithTimeout(BufferBlock<IStreamEvent> collector, IStreamVertex vertex)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var receive = collector.ReceiveAsync(cts.Token);
            var finished = await Task.WhenAny(receive, vertex.Completion);
            if (finished == vertex.Completion)
            {
                await vertex.Completion;
                throw new InvalidOperationException("Vertex completed without emitting");
            }
            return await receive;
        }

        private static StreamMessage<Payload> Message(string name) => new StreamMessage<Payload>(new Payload(name), 1);

        // Sends the first message, waits until the vertex is busy with it, queues the rest and lets the vertex go on
        private static async Task SendQueued(RecordingVertex vertex, params (int Target, IStreamEvent Event)[] events)
        {
            await vertex.Targets[events[0].Target].SendAsync(events[0].Event);
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            for (int i = 1; i < events.Length; i++)
            {
                await vertex.Targets[events[i].Target].SendAsync(events[i].Event);
            }
            // An input hands its events over on a task of its own, the vertex goes on when all of them wait
            var watch = Stopwatch.StartNew();
            while (vertex.Queued < events.Length - 1)
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10), $"Only {vertex.Queued} of {events.Length - 1} events were queued");
                await Task.Delay(1);
            }
            vertex.Gate.SetResult();
        }

        private static async Task<List<string>> ReceiveNames(BufferBlock<IStreamEvent> collector, RecordingVertex vertex, int count)
        {
            var names = new List<string>();
            for (int i = 0; i < count; i++)
            {
                var received = await ReceiveWithTimeout(collector, vertex);
                names.Add(received is StreamMessage<Payload> message ? message.Data.Name : received.GetType().Name);
            }
            return names;
        }

        [Fact]
        public async Task MessagesQueuedTogetherComeAsOneRoundInTheOrderTheyArrived()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (1, Message("b")), (0, Message("c")), (1, Message("d")));

            var outputs = await ReceiveNames(collector, vertex, 4);
            Assert.Equal("out-a", outputs[0]);
            Assert.Equal(2, vertex.Calls.Count);
            Assert.Equal("0:a", vertex.Calls[0]);
            // The inputs hand over on tasks of their own, so only the order within an input is given
            var round = vertex.Calls[1].Split(',');
            Assert.Equal(new[] { "0:c", "1:b", "1:d" }, round.OrderBy(x => x, StringComparer.Ordinal));
            Assert.True(Array.IndexOf(round, "1:b") < Array.IndexOf(round, "1:d"));
            Assert.Equal(round.Select(x => "out-" + x.Substring(2)), outputs.Skip(1));
        }

        [Fact]
        public async Task AMessageWithNothingBehindItIsHandledOnItsOwn()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (1, Message("b")));

            Assert.Equal(new[] { "out-a", "out-b" }, await ReceiveNames(collector, vertex, 2));
            Assert.Equal(new[] { "0:a", "1:b" }, vertex.Calls);
        }

        [Fact]
        public async Task TheMessagesOfTwoInputsCanBeInOneRoundEachInItsOwnOrder()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (0, Message("b0")), (1, Message("b1")), (0, Message("c0")), (1, Message("c1")), (0, Message("d0")), (1, Message("d1")));

            await ReceiveNames(collector, vertex, 7);
            var round = vertex.Calls[1].Split(',');
            Assert.Equal(2, vertex.Calls.Count);
            Assert.Equal(new[] { "0:b0", "0:c0", "0:d0" }, round.Where(x => x.StartsWith("0:")));
            Assert.Equal(new[] { "1:b1", "1:c1", "1:d1" }, round.Where(x => x.StartsWith("1:")));
        }

        [Fact]
        public async Task ARoundEndsAtAnEventThatIsNotData()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            // The trigger arrives between c and d, no message of its input is taken past it
            await SendQueued(vertex, (0, Message("a")), (0, Message("b")), (0, Message("c")), (0, new TriggerEvent("t", null)), (0, Message("d")), (0, Message("e")));

            Assert.Equal(new[] { "out-a", "out-b", "out-c", "out-d", "out-e" }, await ReceiveNames(collector, vertex, 5));
            Assert.Equal(new[] { "0:a", "0:b,0:c", "trigger", "0:d,0:e" }, vertex.Calls);
        }

        [Fact]
        public async Task NoMessageIsTakenPastACheckpoint()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (0, Message("b")), (1, Message("c")), (0, new Checkpoint(0, 1, 1)), (1, Message("d")), (1, new Checkpoint(0, 1, 1)));

            // Every message was sent ahead of the checkpoint of its own input, so all of them are handled before the checkpoint goes on
            var outputs = await ReceiveNames(collector, vertex, 5);
            Assert.Equal("out-a", outputs[0]);
            Assert.Equal(new[] { "out-b", "out-c", "out-d" }, outputs.Skip(1).Take(3).OrderBy(x => x, StringComparer.Ordinal));
            Assert.Equal("Checkpoint", outputs[4]);
            var later = string.Join(",", vertex.Calls.Skip(1)).Split(',');
            Assert.True(Array.IndexOf(later, "1:c") < Array.IndexOf(later, "1:d"));
        }

        [Fact]
        public async Task ARoundHoldsNoMoreMessagesThanItsLimit()
        {
            var vertex = new RoundVertex(maxMessages: 3);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (0, Message("b")), (0, Message("c")), (0, Message("d")), (0, Message("e")), (0, Message("f")));

            await ReceiveNames(collector, vertex, 6);
            Assert.Equal(new[] { "0:a", "0:b,0:c,0:d", "0:e,0:f" }, vertex.Calls);
        }

        [Fact]
        public async Task ARoundTakesNoMoreFromAnInputThanItsCostLimit()
        {
            var vertex = new RoundVertex(maxMessages: 100, maxCost: 3);
            var collector = await SetupVertex(vertex);

            // bb and c make three, dd would make five. The message of five is over the limit on its own and is still taken, nothing more with it
            await SendQueued(vertex, (0, Message("a")), (0, Message("bb")), (0, Message("c")), (0, Message("dd")), (0, Message("eeeee")), (0, Message("f")));

            await ReceiveNames(collector, vertex, 6);
            Assert.Equal(new[] { "0:a", "0:bb,0:c", "0:dd", "0:eeeee", "0:f" }, vertex.Calls);
        }

        [Fact]
        public async Task WithoutARoundSizeEveryMessageIsHandledOnItsOwn()
        {
            var vertex = new RoundVertex(maxMessages: 1);
            var collector = await SetupVertex(vertex);

            // One input, the order across inputs is not given
            await SendQueued(vertex, (0, Message("a")), (0, Message("b")), (0, Message("c")));

            Assert.Equal(new[] { "out-a", "out-b", "out-c" }, await ReceiveNames(collector, vertex, 3));
            Assert.Equal(new[] { "0:a", "0:b", "0:c" }, vertex.Calls);
        }

        [Fact]
        public async Task AVertexThatDoesNotHandleRoundsGetsTheirMessagesOneByOne()
        {
            var vertex = new RecordingVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            // One input, the order across inputs is not given
            await SendQueued(vertex, (0, Message("a")), (0, Message("b")), (0, Message("c")));

            Assert.Equal(new[] { "out-a", "out-b", "out-c" }, await ReceiveNames(collector, vertex, 3));
            Assert.Equal(new[] { "0:a", "0:b", "0:c" }, vertex.Calls);
        }

        [Fact]
        public async Task TheMessagesOfARoundAreReturnedWhenItsOutputIsRead()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);
            var b = Message("b");
            var c = Message("c");

            await SendQueued(vertex, (0, Message("a")), (0, b), (1, c));
            var outputs = new List<StreamMessage<Payload>>();
            for (int i = 0; i < 3; i++)
            {
                outputs.Add(Assert.IsType<StreamMessage<Payload>>(await ReceiveWithTimeout(collector, vertex)));
            }
            // The returns come when the vertex asks the round for more and gets none, a checkpoint through both inputs is behind that
            await vertex.Targets[0].SendAsync(new Checkpoint(0, 1, 1));
            await vertex.Targets[1].SendAsync(new Checkpoint(0, 1, 1));
            Assert.IsAssignableFrom<ICheckpointEvent>(await ReceiveWithTimeout(collector, vertex));

            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
            // One link, so each output is rented once
            Assert.All(outputs, x => Assert.Equal(1, x.Data.Rented));
        }

        [Fact]
        public async Task AFailureInARoundFaultsTheVertex()
        {
            var vertex = new RoundVertex(maxMessages: 100) { FailOn = "c" };
            await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (0, Message("b")), (1, Message("c")));

            var failure = await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains("Injected failure in a round", failure.ToString());
        }

        [Fact]
        public async Task AFailedRoundReturnsItsInputRents()
        {
            var vertex = new RoundVertex(maxMessages: 100) { FailOn = "c" };
            await SetupVertex(vertex);
            var b = Message("b");
            var c = Message("c");
            b.Data.Rent(1);
            c.Data.Rent(1);

            await SendQueued(vertex, (0, Message("a")), (0, b), (1, c));
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains("Injected failure in a round", failure.ToString());
            await vertex.DisposeAsync();

            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
        }

        [Fact]
        public async Task AStoppedVertexReleasesMessagesWaitingInItsFunnel()
        {
            var vertex = new RoundVertex(maxMessages: 1);
            await SetupVertex(vertex);
            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var b = Message("b");
            var c = Message("c");
            b.Data.Rent(1);
            c.Data.Rent(1);
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[0].SendAsync(c);
            var deadline = Stopwatch.StartNew();
            while (vertex.Queued != 2)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Yield();
            }

            vertex.Complete();
            vertex.Gate.SetResult();
            try
            {
                await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (OperationCanceledException)
            {
            }
            // Before the vertex is disposed: completing it is what lets go of them
            Assert.Equal(0, vertex.Queued);
            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
            await vertex.DisposeAsync();

            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
        }

        [Fact]
        public async Task AFaultedVertexReleasesMessagesWaitingInItsFunnel()
        {
            var vertex = new RoundVertex(maxMessages: 1);
            await SetupVertex(vertex);
            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var b = Message("b");
            var c = Message("c");
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[0].SendAsync(c);
            var deadline = Stopwatch.StartNew();
            while (vertex.Queued != 2)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Delay(1);
            }

            vertex.Fault(new InvalidOperationException("Faulted from outside"));
            vertex.Gate.SetResult();
            await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));

            Assert.Equal(0, vertex.Queued);
            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
        }

        [Fact]
        public async Task ACheckpointIsSentOnceEveryInputHasGivenItsOwn()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (1, Message("b")), (0, new Checkpoint(0, 1, 1)));
            Assert.Equal(new[] { "out-a", "out-b" }, await ReceiveNames(collector, vertex, 2));

            // The checkpoint gates its input until the other one has given its own
            var gated = vertex.Targets[0].SendAsync(Message("gated"));
            Assert.False(gated.IsCompleted, "The checkpoint did not gate its input");
            await vertex.Targets[1].SendAsync(new Checkpoint(0, 1, 1));

            Assert.Equal(new[] { "Checkpoint", "out-gated" }, await ReceiveNames(collector, vertex, 2));
            Assert.True(await gated);
        }

        [Fact]
        public async Task TheVertexCompletesWhenItsInputsDo()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            await SendQueued(vertex, (0, Message("a")), (1, Message("b")));
            await ReceiveNames(collector, vertex, 2);
            vertex.Complete();

            // Completing cancels what is pending, as in the vertex that takes one message at a time
            await Task.WhenAny(vertex.Completion, Task.Delay(TimeSpan.FromSeconds(10)));
            Assert.True(vertex.Completion.IsCompleted);
        }

        [Fact]
        public async Task AVertexThatFaultsOnItsOwnLetsGoOfWhatWaitsAndTakesNoMore()
        {
            var vertex = new RoundVertex(maxMessages: 1, boundedCapacity: 2) { FailOn = "a" };
            await SetupVertex(vertex);
            var b = Message("b");
            var c = Message("c");
            var d = Message("d");
            var e = Message("e");

            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[0].SendAsync(c);
            var deadline = Stopwatch.StartNew();
            while (vertex.Queued != 2)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Delay(1);
            }
            // With two events waiting the queue is full, these stay with the inputs
            await vertex.Targets[0].SendAsync(d);
            await vertex.Targets[1].SendAsync(e);

            vertex.Gate.SetResult();
            await Assert.ThrowsAnyAsync<InvalidOperationException>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            // Before the vertex is disposed, which lets go of them too
            deadline.Restart();
            while (b.Data.Returned != 1 || c.Data.Returned != 1)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Delay(1);
            }
            Assert.Equal(0, vertex.Queued);
            await vertex.DisposeAsync();

            Assert.Equal(0, vertex.Queued);
            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
        }

        [Fact]
        public async Task AFaultedInputTargetReleasesMessagesWaitingInTheFunnel()
        {
            var vertex = new RoundVertex(maxMessages: 1);
            await SetupVertex(vertex);
            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var b = Message("b");
            var c = Message("c");
            b.Data.Rent(1);
            c.Data.Rent(1);
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[0].SendAsync(c);
            var deadline = Stopwatch.StartNew();
            while (vertex.Queued != 2)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Yield();
            }

            vertex.Targets[0].Fault(new InvalidOperationException("Upstream target faulted"));
            await Assert.ThrowsAnyAsync<Exception>(() => vertex.Targets[0].Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            // The gate stays closed until the fault has reached the block, which then takes nothing more
            var block = (ITargetBlock<KeyValuePair<int, IStreamEvent>>)typeof(MultipleInputRoundVertex<Payload>)
                .GetField("_transformBlock", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(vertex)!;
            var faulted = Stopwatch.StartNew();
            while (block.OfferMessage(new DataflowMessageHeader(1), default, null, false) != DataflowMessageStatus.DecliningPermanently)
            {
                Assert.True(faulted.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Delay(1);
            }
            vertex.Gate.SetResult();
            var thrown = await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains("Upstream target faulted", thrown.ToString());
            await vertex.DisposeAsync();

            // Only the message that was running was handled, the two that waited were let go of
            Assert.Equal(new[] { "0:a" }, vertex.Calls);
            Assert.Equal(0, vertex.Queued);
            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
        }

        [Fact]
        public async Task DisposingWaitsForTheReleaseTheEndOfTheBlockStarted()
        {
            var vertex = new RoundVertex(maxMessages: 1, boundedCapacity: 2) { FailOn = "a" };
            await SetupVertex(vertex);
            var b = Message("b");
            b.Data.Rent(1);
            var returning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var letReturn = new ManualResetEventSlim(false);
            b.Data.Returning = () =>
            {
                returning.TrySetResult();
                letReturn.Wait(TimeSpan.FromSeconds(10));
            };

            var funnel = (BufferBlock<KeyValuePair<int, IStreamEvent>>)typeof(MultipleInputRoundVertex<Payload>)
                .GetField("_funnel", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(vertex)!;
            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vertex.Targets[0].SendAsync(b);
            Assert.True(await funnel.OutputAvailableAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(1, vertex.Queued);

            vertex.Gate.SetResult();
            // The end of the block has taken b and is returning it
            await returning.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await funnel.Completion.WaitAsync(TimeSpan.FromSeconds(10));
            try
            {
                var disposing = vertex.DisposeAsync().AsTask();
                Assert.False(disposing.IsCompleted);
                letReturn.Set();
                await disposing.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.Equal(1, b.Data.Returned);
            }
            finally
            {
                letReturn.Set();
            }
        }

        [Fact]
        public async Task TheVertexCompletesWhenAllItsInputsHave()
        {
            var vertex = new RoundVertex(maxMessages: 100);
            var collector = await SetupVertex(vertex);

            vertex.Targets[0].Complete();
            // One input that has completed does not close the others
            await SendQueued(vertex, (1, Message("b")));
            await ReceiveNames(collector, vertex, 1);
            Assert.False(vertex.Completion.IsCompleted);

            vertex.Targets[1].Complete();
            await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task DisposingAVertexCompletesItsInputsAndFunnelBeforeDraining()
        {
            var vertex = new RoundVertex(maxMessages: 1, boundedCapacity: 2);
            await SetupVertex(vertex);
            var b = Message("b");
            var c = Message("c");
            var d = Message("d");
            var e = Message("e");
            b.Data.Rent(1);
            c.Data.Rent(1);
            d.Data.Rent(1);
            e.Data.Rent(1);

            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[0].SendAsync(c);
            var deadline = Stopwatch.StartNew();
            while (vertex.Queued != 2)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Yield();
            }
            // Fill Targets[0] (boundedCapacity: 2) behind the full funnel so a third offer waits on Targets[0]
            await vertex.Targets[0].SendAsync(d);
            await vertex.Targets[0].SendAsync(e);
            Task<bool> waitingOnInput = vertex.Targets[0].SendAsync(Message("f"));
            Assert.False(waitingOnInput.IsCompleted);

            try
            {
                await vertex.DisposeAsync();
                bool acceptedAfterDispose = await waitingOnInput.WaitAsync(TimeSpan.FromSeconds(10));

                Assert.False(acceptedAfterDispose);
                Assert.Equal(0, vertex.Queued);
                Assert.Equal(1, b.Data.Returned);
                Assert.Equal(1, c.Data.Returned);
            }
            finally
            {
                vertex.Gate.TrySetResult();
            }
        }

        private sealed class DisposeDuringConsumeSource : ISourceBlock<KeyValuePair<int, IStreamEvent>>
        {
            private readonly RecordingVertex _vertex;
            private readonly KeyValuePair<int, IStreamEvent> _message;
            private bool _consumed;
            public readonly TaskCompletionSource Consumed = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task? Disposing;

            public DisposeDuringConsumeSource(RecordingVertex vertex, IStreamEvent message)
            {
                _vertex = vertex;
                _message = new KeyValuePair<int, IStreamEvent>(0, message);
            }

            public Task Completion => Consumed.Task;
            public void Complete() { }
            public void Fault(Exception exception) => Consumed.TrySetException(exception);
            public IDisposable LinkTo(ITargetBlock<KeyValuePair<int, IStreamEvent>> target, DataflowLinkOptions linkOptions) => throw new NotSupportedException();
            public bool ReserveMessage(DataflowMessageHeader header, ITargetBlock<KeyValuePair<int, IStreamEvent>> target) => !_consumed;
            public void ReleaseReservation(DataflowMessageHeader header, ITargetBlock<KeyValuePair<int, IStreamEvent>> target) { }

            public KeyValuePair<int, IStreamEvent> ConsumeMessage(DataflowMessageHeader header, ITargetBlock<KeyValuePair<int, IStreamEvent>> target, out bool messageConsumed)
            {
                messageConsumed = !_consumed;
                if (!messageConsumed)
                {
                    return default;
                }
                _consumed = true;
                // Disposal starts while the consume holds the message
                Disposing = _vertex.DisposeAsync().AsTask();
                Consumed.TrySetResult();
                return _message;
            }
        }

        [Fact]
        public async Task DisposingDuringPostponedConsumptionDoesNotPublishAfterTheDrain()
        {
            var vertex = new RoundVertex(maxMessages: 1, boundedCapacity: 2);
            await SetupVertex(vertex);
            await vertex.Targets[0].SendAsync(Message("a"));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var b = Message("b");
            var c = Message("c");
            var late = Message("late");
            b.Data.Rent(1);
            c.Data.Rent(1);
            late.Data.Rent(1);
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[0].SendAsync(c);
            var deadline = Stopwatch.StartNew();
            while (vertex.Queued != 2)
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Yield();
            }
            var funnel = (BufferBlock<KeyValuePair<int, IStreamEvent>>)typeof(MultipleInputRoundVertex<Payload>)
                .GetField("_funnel", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(vertex)!;
            var source = new DisposeDuringConsumeSource(vertex, late);
            try
            {
                Assert.Equal(DataflowMessageStatus.Postponed, ((ITargetBlock<KeyValuePair<int, IStreamEvent>>)funnel)
                    .OfferMessage(new DataflowMessageHeader(1), default, source, consumeToAccept: true));
                Assert.True(funnel.TryReceive(out var removed));
                ((StreamMessage<Payload>)removed.Value).Data.Return();
                await source.Consumed.Task.WaitAsync(TimeSpan.FromSeconds(10));
                await source.Disposing!.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.False(await funnel.OutputAvailableAsync().WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.Equal(1, late.Data.Returned);
                Assert.Equal(0, vertex.Queued);
            }
            finally
            {
                while (funnel.TryReceive(out var remaining))
                {
                    ((StreamMessage<Payload>)remaining.Value).Data.Return();
                }
                vertex.Gate.TrySetResult();
                vertex.Complete();
                try
                {
                    await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }
}
