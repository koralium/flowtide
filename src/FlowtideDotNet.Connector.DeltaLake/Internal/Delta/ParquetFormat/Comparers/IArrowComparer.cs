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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    /// <summary>
    /// Comparer interface to help search for deleted values in the saved files
    /// </summary>
    internal interface IArrowComparer
    {
        /// <summary>
        /// Check if the value from the left array is equal to the value in the right array
        /// </summary>
        /// <param name="index"></param>
        /// <param name="array"></param>
        /// <param name="otherArray"></param>
        /// <returns></returns>
        bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray);

        /// <summary>
        /// Iterates over the right array to find the first occurance of the value from the left array
        /// </summary>
        /// <param name="toFindIndex"></param>
        /// <param name="toFindFrom"></param>
        /// <param name="searchIndex"></param>
        /// <param name="searchLength"></param>
        /// <param name="toFindIn"></param>
        /// <returns></returns>
        int FindOccurance(
            int toFindIndex, 
            IArrowArray toFindFrom, 
            int searchIndex, 
            int searchLength, 
            IArrowArray toFindIn,
            int globalOffset,
            IDeleteVector deleteVector);
    }
}
