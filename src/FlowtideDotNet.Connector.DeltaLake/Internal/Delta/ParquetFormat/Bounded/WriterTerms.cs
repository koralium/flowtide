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

using ParquetSharp;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    // One leaf's held buffers, its sink, and its largest grow-only block
    internal readonly record struct LeafWriterTerms(long Held, long Sink, long Overlap);

    // Native writer memory of one row group, without per-file terms
    internal static class WriterTerms
    {
        // Arrow closes a page by 20,000 rows
        public const long RowsPerPage = 20_000;

        public static LeafWriterTerms Leaf(Compression compression, long dataPageSize, long f, long levels, long batchMax, long rowMax, int streams, long rows)
        {
            checked
            {
                var pageValues = Math.Min(f, dataPageSize + batchMax);
                var pageLevels = Math.Min(levels, RowsPerPage * Math.Max(1, rowMax));
                var rle = streams * (pageLevels / 2 + 128);
                var page = pageValues + rle;
                var scratch = NativeConstants.HasScratch(compression) ? NativeConstants.Compressed(compression, page) : 0;
                var pages = (f + dataPageSize - 1) / dataPageSize + (rows + RowsPerPage - 1) / RowsPerPage;
                var sink = NativeConstants.Compressed(compression, f + streams * levels / 2) + 1024 * pages;
                // Every grow-only buffer's content, the largest is the one block a reallocation can overlap
                var overlap = Math.Max(Math.Max(2 * pageLevels, rle), Math.Max(Math.Max(pageValues, page), Math.Max(scratch, sink)));
                return new LeafWriterTerms(4 * streams * pageLevels + rle + 3 * pageValues + page + scratch, sink, overlap);
            }
        }

        // The leaves summed with one write's level buffers
        public static long Group(long held, long sinks, long overlap, long path)
        {
            return checked(held + 2 * sinks + Math.Max(overlap, path) + path);
        }
    }
}
