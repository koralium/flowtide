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

using System.Runtime.ExceptionServices;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    /// <summary>
    /// The few pages one reader holds at a time. Used by one thread.
    /// </summary>
    internal sealed class PageWindow<V> : IDisposable
        where V : class, ICacheObject
    {
        private readonly ImmutablePageClient<V> _client;
        private readonly V?[] _pages;
        private int _count;
        private long _hits;
        private bool _disposed;

        internal PageWindow(ImmutablePageClient<V> client, int size)
        {
            _client = client;
            _pages = new V?[size];
        }

        public int Count => Volatile.Read(ref _count);

        // Cache hits of this reader, read by the client
        internal long Hits => Volatile.Read(ref _hits);

        internal void Hit()
        {
            Volatile.Write(ref _hits, _hits + 1);
        }

        public int Size => _pages.Length;

        internal void ThrowIfFull()
        {
            if (_count == _pages.Length)
            {
                throw new InvalidOperationException($"A reader holds at most {_pages.Length} pages, release one first.");
            }
        }

        // The page is already rented for this window
        internal void Hold(V page)
        {
            _pages[_count] = page;
            Volatile.Write(ref _count, _count + 1);
        }

        /// <summary>
        /// Gives one page back.
        /// </summary>
        public void Release(V page)
        {
            for (int i = 0; i < _count; i++)
            {
                if (ReferenceEquals(_pages[i], page))
                {
                    int last = _count - 1;
                    _pages[i] = _pages[last];
                    _pages[last] = null;
                    Volatile.Write(ref _count, last);
                    page.Return();
                    return;
                }
            }
            throw new InvalidOperationException("The page is not held by this reader.");
        }

        /// <summary>
        /// Gives every held page back.
        /// </summary>
        public void ReleaseAll()
        {
            Exception? failure = null;
            int count = _count;
            Volatile.Write(ref _count, 0);
            for (int i = 0; i < count; i++)
            {
                var page = _pages[i]!;
                _pages[i] = null;
                try
                {
                    page.Return();
                }
                catch (Exception e)
                {
                    // The other pages are still returned, then this is thrown
                    failure ??= e;
                }
            }
            if (failure != null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            try
            {
                ReleaseAll();
            }
            finally
            {
                // Also when a page cannot be returned
                _client.RemoveWindow(this);
            }
        }
    }
}
