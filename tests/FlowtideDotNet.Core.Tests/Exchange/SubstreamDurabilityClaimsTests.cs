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

using FlowtideDotNet.Core.Operators.Exchange;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    public class SubstreamDurabilityClaimsTests
    {
        /// <summary>
        /// Substreams that only exchange claims with their direct peers, safety checked per delivery.
        /// </summary>
        private sealed class Network
        {
            private readonly Dictionary<string, SubstreamDurabilityClaims> _nodes = new Dictionary<string, SubstreamDurabilityClaims>();
            private readonly Dictionary<string, List<string>> _edges = new Dictionary<string, List<string>>();
            private readonly Dictionary<string, long> _durable = new Dictionary<string, long>();
            private readonly List<(string From, string To, SubstreamDurabilityClaim Claim)> _inFlight = new List<(string, string, SubstreamDurabilityClaim)>();
            private readonly Random _random;

            public Network(int seed, (string A, string B)[] edges, int? distance = null)
            {
                _random = new Random(seed);
                foreach (var (a, b) in edges)
                {
                    Connect(a, b);
                    Connect(b, a);
                }
                // The real one by default: the tightest bound is the one that has to hold.
                var hops = distance ?? _edges.Keys.Max(HopsToTheFarthest);
                foreach (var node in _edges.Keys)
                {
                    _nodes[node] = new SubstreamDurabilityClaims(_edges[node], hops);
                    _durable[node] = SubstreamDurabilityClaims.Unknown;
                }
            }

            private int HopsToTheFarthest(string start)
            {
                var hops = new Dictionary<string, int>() { { start, 0 } };
                var queue = new Queue<string>();
                queue.Enqueue(start);
                while (queue.Count > 0)
                {
                    var current = queue.Dequeue();
                    foreach (var neighbour in _edges[current])
                    {
                        if (hops.TryAdd(neighbour, hops[current] + 1))
                        {
                            queue.Enqueue(neighbour);
                        }
                    }
                }
                return hops.Values.Max();
            }

            public IEnumerable<string> Names => _nodes.Keys;

            public double DropRate { get; set; }

            public SubstreamDurabilityClaims this[string name] => _nodes[name];

            public void Durable(string node, long version)
            {
                _durable[node] = Math.Max(_durable[node], version);
                Send(node, _nodes[node].SetLocalDurable(version));
            }

            public void ResendEverything()
            {
                foreach (var node in _nodes.Keys)
                {
                    Send(node, _nodes[node].CurrentClaims(), droppable: false);
                }
            }

            public void RunToQuiescence()
            {
                while (_inFlight.Count > 0)
                {
                    // Any order, a claim can overtake another.
                    var index = _random.Next(_inFlight.Count);
                    var (from, to, claim) = _inFlight[index];
                    _inFlight.RemoveAt(index);
                    Send(to, _nodes[to].ApplyPeerClaim(from, claim));
                    AssertSafe();
                }
            }

            public void AssertAgreed(long expected)
            {
                foreach (var node in _nodes)
                {
                    Assert.True(expected == node.Value.Agreed, $"{node.Key} agreed on {node.Value.Agreed}, expected {expected}.");
                }
            }

            private void AssertSafe()
            {
                // Nobody may believe in a version some substream is not durable at.
                var lowest = _durable.Values.Min();
                foreach (var node in _nodes)
                {
                    Assert.True(node.Value.Agreed <= lowest, $"{node.Key} agreed on {node.Value.Agreed} while the lowest durable version is {lowest}.");
                    Assert.True(node.Value.KnownDurable <= lowest, $"{node.Key} knows {node.Value.KnownDurable} while the lowest durable version is {lowest}.");
                }
                // The second pass: whoever agreed on a version, everyone already knows the group is durable at it.
                var highestAgreed = _nodes.Values.Max(n => n.Agreed);
                foreach (var node in _nodes)
                {
                    Assert.True(node.Value.HighestKnownDurable >= highestAgreed, $"someone agreed on {highestAgreed} while {node.Key} only ever knew {node.Value.HighestKnownDurable}.");
                }
            }

            private void Send(string from, IReadOnlyList<SubstreamDurabilityClaim> claims, bool droppable = true)
            {
                foreach (var claim in claims)
                {
                    foreach (var to in _edges[from])
                    {
                        if (droppable && _random.NextDouble() < DropRate)
                        {
                            continue;
                        }
                        _inFlight.Add((from, to, claim));
                    }
                }
            }

            private void Connect(string from, string to)
            {
                if (!_edges.TryGetValue(from, out var peers))
                {
                    peers = new List<string>();
                    _edges.Add(from, peers);
                }
                if (!peers.Contains(to))
                {
                    peers.Add(to);
                }
            }
        }

        private static (string, string)[] Chain(int length)
        {
            return Enumerable.Range(1, length - 1).Select(i => ($"s{i}", $"s{i + 1}")).ToArray();
        }

        private static (string, string)[] Ring(int length)
        {
            return Chain(length).Append(($"s{length}", "s1")).ToArray();
        }

        private static (string, string)[] Star(int leaves)
        {
            return Enumerable.Range(1, leaves).Select(i => ("hub", $"leaf{i}")).ToArray();
        }

        public static IEnumerable<object[]> Topologies()
        {
            yield return new object[] { "chain of 2", Chain(2) };
            yield return new object[] { "chain of 3", Chain(3) };
            yield return new object[] { "chain of 5", Chain(5) };
            yield return new object[] { "ring of 4", Ring(4) };
            yield return new object[] { "ring of 5", Ring(5) };
            yield return new object[] { "star of 4", Star(4) };
            yield return new object[] { "two triangles sharing a node", new[] { ("a", "b"), ("b", "c"), ("c", "a"), ("c", "d"), ("d", "e"), ("e", "c") } };
        }

        [Theory]
        [MemberData(nameof(Topologies))]
        public void EveryoneAgreesOnTheLowestDurableVersion(string name, (string, string)[] edges)
        {
            for (int seed = 0; seed < 25; seed++)
            {
                var network = new Network(seed, edges);
                var names = network.Names.ToList();

                // All but one reach 5, the last one only 4.
                foreach (var node in names.Skip(1))
                {
                    network.Durable(node, 5);
                }
                network.RunToQuiescence();
                network.AssertAgreed(SubstreamDurabilityClaims.Unknown);

                network.Durable(names[0], 4);
                network.RunToQuiescence();
                network.AssertAgreed(4);

                network.Durable(names[0], 5);
                network.RunToQuiescence();
                network.AssertAgreed(5);
                Assert.NotNull(name);
            }
        }

        [Theory]
        [MemberData(nameof(Topologies))]
        public void VersionsInterleavedInAnyOrderStaySafe(string name, (string, string)[] edges)
        {
            for (int seed = 0; seed < 25; seed++)
            {
                var network = new Network(seed, edges);
                var names = network.Names.ToList();
                var random = new Random(seed);

                // Commits land while older claims are still in flight.
                for (long version = 0; version <= 6; version++)
                {
                    foreach (var node in names.OrderBy(_ => random.Next()))
                    {
                        network.Durable(node, version);
                        if (random.Next(3) == 0)
                        {
                            network.RunToQuiescence();
                        }
                    }
                }
                network.RunToQuiescence();
                network.AssertAgreed(6);
                Assert.NotNull(name);
            }
        }

        [Theory]
        [MemberData(nameof(Topologies))]
        public void DroppedClaimsAreRepairedByResending(string name, (string, string)[] edges)
        {
            for (int seed = 0; seed < 25; seed++)
            {
                var network = new Network(seed, edges) { DropRate = 0.5 };
                foreach (var node in network.Names.ToList())
                {
                    network.Durable(node, 3);
                }
                network.RunToQuiescence();

                // What a timer does while a substream waits for the agreement.
                network.DropRate = 0;
                for (int round = 0; round < edges.Length + 2; round++)
                {
                    network.ResendEverything();
                    network.RunToQuiescence();
                }
                network.AssertAgreed(3);
                Assert.NotNull(name);
            }
        }

        [Fact]
        public void ASingleSubstreamAgreesWithItself()
        {
            var claims = new SubstreamDurabilityClaims(Array.Empty<string>(), 0);
            Assert.Equal(SubstreamDurabilityClaims.Unknown, claims.Agreed);

            var grown = claims.SetLocalDurable(7);

            Assert.Equal(7, claims.Agreed);
            Assert.Equal(new[] { new SubstreamDurabilityClaim(0, 7) }, grown);
        }

        [Fact]
        public void VersionZeroIsAVersionNotUnknown()
        {
            var network = new Network(1, Chain(3));
            foreach (var node in network.Names.ToList())
            {
                network.Durable(node, 0);
            }
            network.RunToQuiescence();
            network.AssertAgreed(0);
        }

        [Fact]
        public void APeerThatNeverClaimsBlocksTheAgreement()
        {
            var network = new Network(1, Chain(3));
            network.Durable("s1", 9);
            network.Durable("s2", 9);
            network.RunToQuiescence();
            network.AssertAgreed(SubstreamDurabilityClaims.Unknown);
        }

        [Fact]
        public void TheFarEndOfAChainIsNeededNotJustTheNeighbour()
        {
            // The proven gap: s3 must not settle on a version because s2 has it.
            var network = new Network(1, Chain(3));
            network.Durable("s3", 2);
            network.Durable("s2", 2);
            network.RunToQuiescence();
            Assert.Equal(SubstreamDurabilityClaims.Unknown, network["s3"].Agreed);

            network.Durable("s1", 2);
            network.RunToQuiescence();
            Assert.Equal(2, network["s3"].Agreed);
        }

        [Fact]
        public void ATooShortDistanceIsCaughtByTheSafetyCheck()
        {
            // Keeps the per delivery check honest: two passes of radius 2 do not span a chain of 7.
            var network = new Network(1, Chain(7), distance: 2);
            foreach (var node in new[] { "s1", "s2", "s3", "s4", "s5", "s6" })
            {
                network.Durable(node, 1);
            }
            var failure = Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => network.RunToQuiescence());
            Assert.Contains("while the lowest durable version is -1", failure.Message);
        }

        [Fact]
        public void AnOlderClaimLowersNothing()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            claims.SetLocalDurable(5);
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(0, 5));
            Assert.Equal(5, claims.KnownDurable);

            Assert.Empty(claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(0, 3)));
            Assert.Empty(claims.SetLocalDurable(4));
            Assert.Equal(5, claims.KnownDurable);
        }

        [Fact]
        public void AHigherRadiusStandsInForALostLowerOne()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            claims.SetLocalDurable(5);

            // Radius 0 from the peer never arrived.
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(1, 5));

            Assert.Equal(5, claims.Agreed);
        }

        [Fact]
        public void CurrentClaimsSendsTheHighestRadiusPerVersion()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            claims.SetLocalDurable(4);
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(0, 4));
            claims.SetLocalDurable(5);

            Assert.Equal(
                new[] { new SubstreamDurabilityClaim(1, 4), new SubstreamDurabilityClaim(0, 5) },
                claims.CurrentClaims());
        }

        [Fact]
        public void ResetForgetsEverythingAndCancelsWaiters()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            claims.SetLocalDurable(5);
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(0, 5));
            var waiter = claims.WhenAgreed(6);

            claims.Reset();

            Assert.True(waiter.IsCanceled);
            Assert.Equal(SubstreamDurabilityClaims.Unknown, claims.Agreed);
            Assert.Empty(claims.CurrentClaims());

            // The version number is reused after the rollback, the old claim must not count.
            claims.SetLocalDurable(5);
            Assert.Equal(SubstreamDurabilityClaims.Unknown, claims.Agreed);
        }

        [Fact]
        public void ResettingAPeerDropsEverythingBuiltOnItsClaims()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            claims.SetLocalDurable(5);
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(1, 5));
            Assert.Equal(5, claims.Agreed);

            claims.ResetPeer("peer");

            // Only what this substream knows about itself is left to claim.
            Assert.Equal(SubstreamDurabilityClaims.Unknown, claims.Agreed);
            Assert.Equal(SubstreamDurabilityClaims.Unknown, claims.KnownDurable);
            Assert.Equal(new[] { new SubstreamDurabilityClaim(0, 5) }, claims.CurrentClaims());
            // What it once knew is kept, someone may have acted on it.
            Assert.Equal(5, claims.HighestKnownDurable);

            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(1, 5));
            Assert.Equal(5, claims.Agreed);
        }

        [Fact]
        public void APeerThatComesBackLowerLeavesNoStaleRadiusBehind()
        {
            // s2 in the middle of s1 - s2 - s3, everyone durable at 5.
            var claims = new SubstreamDurabilityClaims(new[] { "s1", "s3" }, 2);
            claims.SetLocalDurable(5);
            claims.ApplyPeerClaim("s1", new SubstreamDurabilityClaim(1, 5));
            claims.ApplyPeerClaim("s3", new SubstreamDurabilityClaim(1, 5));
            Assert.Equal(5, claims.KnownDurable);

            // s3 restarted on its own and came back at 4, version 5 will be written again.
            claims.ResetPeer("s3");
            var grown = claims.ApplyPeerClaim("s3", new SubstreamDurabilityClaim(0, 4));

            Assert.Equal(new[] { new SubstreamDurabilityClaim(1, 4) }, grown);
            Assert.DoesNotContain(claims.CurrentClaims(), c => c.Radius > 0 && c.Version > 4);
            Assert.True(claims.KnownDurable <= 4, $"knows {claims.KnownDurable} although s3 is only durable at 4.");
            Assert.True(claims.Agreed <= 4, $"agreed on {claims.Agreed} although s3 is only durable at 4.");
        }

        [Fact]
        public async Task WhenAgreedCompletesOnceTheVersionIsReached()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            claims.SetLocalDurable(3);
            var waiter = claims.WhenAgreed(3);
            Assert.False(waiter.IsCompleted);

            // The peer being durable is the first pass, the wait is for the second.
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(0, 3));
            Assert.False(waiter.IsCompleted);
            claims.ApplyPeerClaim("peer", new SubstreamDurabilityClaim(1, 3));

            await waiter.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(claims.WhenAgreed(2).IsCompletedSuccessfully);
        }

        [Fact]
        public void AClaimFromSomeoneWhoIsNotAPeerIsRejected()
        {
            var claims = new SubstreamDurabilityClaims(new[] { "peer" }, 1);
            Assert.Throws<ArgumentException>(() => claims.ApplyPeerClaim("stranger", new SubstreamDurabilityClaim(0, 1)));
            Assert.Throws<ArgumentException>(() => claims.ResetPeer("stranger"));
        }

        [Fact]
        public void APeerAtNoDistanceIsRejected()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new SubstreamDurabilityClaims(new[] { "a" }, 0));
        }

        /// <summary>
        /// Everyone exchanges data with everyone, the common shape: two rounds whatever the size.
        /// </summary>
        [Fact]
        public void AFullyConnectedGroupAgreesInTwoRounds()
        {
            var names = Enumerable.Range(1, 10).Select(i => $"s{i}").ToArray();
            var edges = (from a in names from b in names where string.CompareOrdinal(a, b) < 0 select (a, b)).ToArray();
            var network = new Network(1, edges);
            foreach (var name in names)
            {
                network.Durable(name, 1);
            }
            network.RunToQuiescence();

            Assert.All(names, name => Assert.Equal(1, network[name].Agreed));
            // Radius 1 is everyone durable, radius 2 everyone knows, nothing beyond is claimed.
            Assert.All(names, name => Assert.Equal(2, network[name].CurrentClaims().Max(c => c.Radius)));
        }
    }
}
