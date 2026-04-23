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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class ColumnSizeInfo
    {
        public ArrowTypeId DataType { get; set; }

        public StructHeader? StructHeader { get; set; }
        
        public int TotalRows { get; set; }

        public int BitWidth { get; set; }

        public int TotalVariableBytes { get; set; }
        
        public List<ColumnSizeInfo>? Children { get; set; }

        private static bool StructHeaderEqual(StructHeader? x, StructHeader? y)
        {
            if (x == null && y == null) return true;
            if (x == null || y == null) return false;
            return x.Value.Equals(y.Value);
        }

        public void Merge(ColumnSizeInfo other)
        {
            if (this.BitWidth > 0 && other.BitWidth > 0)
            {
                // Up bithwidth if required
                this.BitWidth = Math.Max(this.BitWidth, other.BitWidth);
            }

            if (this.DataType == other.DataType && StructHeaderEqual(StructHeader, other.StructHeader))
            {
                this.TotalRows += other.TotalRows;
                this.TotalVariableBytes += other.TotalVariableBytes;

                if (this.DataType == ArrowTypeId.Union)
                {
                    MergeUnionChildren(other);
                }
                else if (this.Children != null && other.Children != null)
                {
                    var minChildCount = Math.Min(this.Children.Count, other.Children.Count);
                    for (int i = 0; i < minChildCount; i++)
                    {
                        this.Children[i].Merge(other.Children[i]);
                    }
                }
                return;
            }

            if (this.DataType != ArrowTypeId.Union)
            {
                PromoteToUnion();
            }

            this.TotalRows += other.TotalRows;

            if (other.DataType == ArrowTypeId.Union)
            {
                MergeUnionChildren(other);
            }
            else
            {
                var matchingChild = this.Children!.FirstOrDefault(c => c.DataType == other.DataType && StructHeaderEqual(c.StructHeader, other.StructHeader));
                if (matchingChild != null)
                {
                    matchingChild.Merge(other);
                }
                else
                {
                    this.Children!.Add(other.Clone());
                }
            }
        }

        private void PromoteToUnion()
        {
            var snapshotChild = new ColumnSizeInfo
            {
                DataType = this.DataType,
                StructHeader = this.StructHeader,
                TotalRows = this.TotalRows,
                TotalVariableBytes = this.TotalVariableBytes,
                BitWidth = this.BitWidth,
                Children = this.Children
            };

            this.DataType = ArrowTypeId.Union;
            this.StructHeader = default;
            this.BitWidth = 0;
            this.Children = new List<ColumnSizeInfo> { snapshotChild };

            this.TotalVariableBytes = 0;
        }

        private void MergeUnionChildren(ColumnSizeInfo otherUnion)
        {
            foreach (var otherChild in otherUnion.Children!)
            {
                var myChild = this.Children!.FirstOrDefault(c => c.DataType == otherChild.DataType && StructHeaderEqual(c.StructHeader, otherChild.StructHeader));
                if (myChild != null)
                {
                    myChild.Merge(otherChild);
                }
                else
                {
                    this.Children!.Add(otherChild.Clone());
                }
            }
        }

        public ColumnSizeInfo Clone()
        {
            return new ColumnSizeInfo
            {
                DataType = this.DataType,
                TotalRows = this.TotalRows,
                TotalVariableBytes = this.TotalVariableBytes,
                StructHeader = this.StructHeader,
                BitWidth = this.BitWidth,
                Children = this.Children?.Select(c => c.Clone()).ToList()
            };
        }
    }
}
