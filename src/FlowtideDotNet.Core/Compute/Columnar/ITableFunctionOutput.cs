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

using FlowtideDotNet.Core.ColumnStore;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    /// <summary>
    /// Sink that a compiled table function appends its generated rows into.
    /// <para>
    /// This replaces the previous contract where a table function returned a fresh
    /// <see cref="EventBatchWeighted"/> for every single input row, which allocated a
    /// column set, weight/iteration lists and batch wrappers per row. Here the columns
    /// and lists are owned by the operator and reused for the whole incoming batch, in
    /// the same spirit as the bulk aggregate operator and the window output builder.
    /// </para>
    /// </summary>
    public interface ITableFunctionOutput
    {
        /// <summary>
        /// The function's output columns, one per column in the table function schema.
        /// For each produced row the function writes one value to every column and then
        /// calls <see cref="CommitRow"/>.
        /// </summary>
        IReadOnlyList<IColumn> Columns { get; }

        /// <summary>
        /// Number of rows committed so far.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Commits a produced row with the given weight and iteration. Must be called
        /// once, after the row's values have been written to <see cref="Columns"/>.
        /// </summary>
        void CommitRow(int weight, uint iteration);

        /// <summary>
        /// Commits <paramref name="count"/> produced rows that all share the same weight and
        /// iteration, after their values have been written to <see cref="Columns"/>. This lets
        /// a function that emits a run of uniform rows (e.g. unnesting a list) fill the
        /// bookkeeping in bulk instead of one <see cref="CommitRow"/> call per row.
        /// </summary>
        void CommitRows(int count, int weight, uint iteration);
    }
}
