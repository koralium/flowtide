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

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// One recovery of a group of substreams. Minted above every wave its minter has seen, so a new failure always outranks the wave the group is in.
    /// </summary>
    public readonly record struct RecoveryWave(long Counter, Guid Id) : IComparable<RecoveryWave>
    {
        /// <summary>
        /// The wave every fresh substream starts in.
        /// </summary>
        public static readonly RecoveryWave None = default;

        public int CompareTo(RecoveryWave other)
        {
            var byCounter = Counter.CompareTo(other.Counter);
            return byCounter != 0 ? byCounter : Id.CompareTo(other.Id);
        }

        public static bool operator >(RecoveryWave left, RecoveryWave right) => left.CompareTo(right) > 0;

        public static bool operator <(RecoveryWave left, RecoveryWave right) => left.CompareTo(right) < 0;

        public static bool operator >=(RecoveryWave left, RecoveryWave right) => left.CompareTo(right) >= 0;

        public static bool operator <=(RecoveryWave left, RecoveryWave right) => left.CompareTo(right) <= 0;

        public override string ToString()
        {
            return $"{Counter}/{Id.ToString("N").Substring(0, 8)}";
        }
    }

    /// <summary>
    /// Which recovery this substream is in, one holder per substream. A failure of the substream itself mints a wave, a peer's
    /// wave is entered when it outranks the current one, and a start joins whatever wave the failure or the peer put it in.
    /// </summary>
    internal sealed class SubstreamRecoveryWaves
    {
        private readonly object _lock = new object();
        private RecoveryWave _current;
        // A wave was minted or entered since the last completed start, the coming start belongs to it. A fresh
        // substream is in init until its first start completed.
        private bool _entered = true;
        // A start began in the current wave: its run may have fetched events in it, its failure needs a new one.
        private bool _startedInCurrent;
        // The wave the last start began in, what a come-down derives from.
        private RecoveryWave _startWave;
        // A run in the current wave ended in a stop, a running peer cannot tell the next start from it.
        private bool _stoppedAfterRun;
        private readonly List<SubstreamCommunicationPoint> _points = new List<SubstreamCommunicationPoint>();

        /// <summary>
        /// Every communication point of this substream, a copy.
        /// </summary>
        public IReadOnlyList<SubstreamCommunicationPoint> Points
        {
            get
            {
                lock (_lock)
                {
                    return _points.ToArray();
                }
            }
        }

        /// <summary>
        /// Adds a communication point of this substream.
        /// </summary>
        public void Register(SubstreamCommunicationPoint point)
        {
            lock (_lock)
            {
                _points.Add(point);
            }
        }

        public RecoveryWave Current
        {
            get
            {
                lock (_lock)
                {
                    return _current;
                }
            }
        }

        /// <summary>
        /// True from entering or minting a wave, or from construction, until the start that follows reached running.
        /// </summary>
        public bool InInit
        {
            get
            {
                lock (_lock)
                {
                    return _entered;
                }
            }
        }

        /// <summary>
        /// True from a stop of a run in the current wave until the wave moves or a start completed.
        /// </summary>
        public bool RestartUnseen
        {
            get
            {
                lock (_lock)
                {
                    return _stoppedAfterRun;
                }
            }
        }

        /// <summary>
        /// True when the wave outranks the current one: the substream is in it now and has to restart into it.
        /// </summary>
        public bool TryEnter(RecoveryWave wave)
        {
            lock (_lock)
            {
                if (wave <= _current)
                {
                    return false;
                }
                _current = wave;
                _entered = true;
                _startedInCurrent = false;
                _stoppedAfterRun = false;
                return true;
            }
        }

        /// <summary>
        /// The wave a failure of this substream belongs to. A run that began in the current wave may have fetched events
        /// the peers will not send again, so its failure is a new wave; a run ended by entering a wave restarts in that one.
        /// </summary>
        public RecoveryWave ForFailure()
        {
            lock (_lock)
            {
                if (!_entered || _startedInCurrent)
                {
                    _current = Mint_NoLock(_current);
                    _entered = true;
                    _startedInCurrent = false;
                    _stoppedAfterRun = false;
                }
                return _current;
            }
        }

        /// <summary>
        /// The wave a start is in: the one a failure or a peer put this substream in, else the one it was in before its
        /// stop. A fresh substream starts in <see cref="RecoveryWave.None"/> like every other fresh one; one with earlier
        /// runs mints above what its peers answer, see the initialize handshake.
        /// </summary>
        public RecoveryWave ForStart()
        {
            lock (_lock)
            {
                _entered = true;
                _startedInCurrent = true;
                _startWave = _current;
                return _current;
            }
        }

        /// <summary>
        /// A new wave above one seen from a peer, for a substream whose own is too low to be noticed.
        /// </summary>
        public RecoveryWave MintAbove(RecoveryWave seen)
        {
            lock (_lock)
            {
                _current = Mint_NoLock(seen > _current ? seen : _current);
                _entered = true;
                _startedInCurrent = false;
                _stoppedAfterRun = false;
                return _current;
            }
        }

        /// <summary>
        /// The wave in which the group comes down to the version it all has. Derived, not random: every substream that comes
        /// down mints the same one, and the ones already at that version restart into it once and send their init again.
        /// </summary>
        public RecoveryWave MintForLowering(long groupVersion)
        {
            lock (_lock)
            {
                // Derived from the wave the start began in, so every call of one start gives the same wave.
                var bytes = new byte[16];
                var seed = _startWave.Id.ToByteArray();
                var version = BitConverter.GetBytes(groupVersion);
                for (int i = 0; i < 16; i++)
                {
                    bytes[i] = (byte)(seed[i] ^ version[i % 8] ^ (byte)(i * 31));
                }
                var lowering = new RecoveryWave(_startWave.Counter + 1, new Guid(bytes));
                if (lowering > _current)
                {
                    _current = lowering;
                    _startedInCurrent = false;
                    _stoppedAfterRun = false;
                }
                _entered = true;
                return _current;
            }
        }

        /// <summary>
        /// Joins a peer's wave without a restart, for a clean handoff back into a running group; the start continues in it.
        /// </summary>
        public void Adopt(RecoveryWave wave)
        {
            lock (_lock)
            {
                if (wave > _current)
                {
                    _current = wave;
                    // The start continues in the adopted wave, it is a start in it.
                    _startedInCurrent = true;
                    _startWave = wave;
                }
            }
        }

        /// <summary>
        /// The start reached running, the next failure is a new recovery.
        /// </summary>
        public void StartCompleted()
        {
            lock (_lock)
            {
                _entered = false;
                _stoppedAfterRun = false;
            }
        }

        /// <summary>
        /// The run ended in a stop: in init again until the next start completed, a peer's handshake must not read it as running.
        /// </summary>
        public void Stopped()
        {
            lock (_lock)
            {
                _stoppedAfterRun = true;
                _entered = true;
            }
        }

        private static RecoveryWave Mint_NoLock(RecoveryWave above)
        {
            return new RecoveryWave(above.Counter + 1, Guid.NewGuid());
        }
    }
}
