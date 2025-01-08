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

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    internal class DataValueComparer : IComparer<IDataValue>
    {
        public static readonly DataValueComparer Instance = new DataValueComparer();

        public int Compare(IDataValue? x, IDataValue? y)
        {
            return CompareTo(x!, y!);
        }

        public static int CompareTo<T1, T2>(T1 x, T2 y)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (x!.Type != y!.Type)
            {
                return x.Type - y.Type;
            }

            switch (x.Type)
            {
                case ArrowTypeId.Null:
                    return 0;
                case ArrowTypeId.Boolean:
                    return x.AsBool.CompareTo(y.AsBool);
                case ArrowTypeId.Int64:
                    return x.AsLong.CompareTo(y.AsLong);
                case ArrowTypeId.Double:
                    return x.AsDouble.CompareTo(y.AsDouble);
                case ArrowTypeId.String:
                    return x.AsString.CompareTo(y.AsString);
                case ArrowTypeId.Binary:
                    return x.AsBinary.SequenceCompareTo(y.AsBinary);
                case ArrowTypeId.Decimal128:
                    return x.AsDecimal.CompareTo(y.AsDecimal);
                case ArrowTypeId.Timestamp:
                    return x.AsTimestamp.CompareTo(y.AsTimestamp);
                case ArrowTypeId.Map:
                    return CompareMaps(x.AsMap, y.AsMap);
                case ArrowTypeId.List:
                    return CompareList(x.AsList, y.AsList);
                default:
                    throw new NotImplementedException();
            }
        }

        private static int CompareMaps(IMapValue x, IMapValue y)
        {
            var xLength = x.GetLength();
            var yLength = y.GetLength();

            if (xLength != yLength)
            {
                return xLength - yLength;
            }

            var xContainer = new DataValueContainer();
            var yContainer = new DataValueContainer();

            for (int i = 0; i < xLength; i++)
            {
                x.GetKeyAt(i, xContainer);
                y.GetKeyAt(i, yContainer);
                int keyCompare = CompareTo(xContainer, yContainer);
                if (keyCompare != 0)
                {
                    return keyCompare;
                }
            }

            for (int i = 0; i < xLength; i++)
            {
                x.GetValueAt(i, xContainer);
                y.GetValueAt(i, yContainer);
                int valCompare = CompareTo(xContainer, yContainer);
                if (valCompare != 0)
                {
                    return valCompare;
                }
            }
            return 0;
        }

        private static int CompareList(IListValue x, IListValue y)
        {
            var xCount = x.Count;
            var yCount = y.Count;

            if (xCount != yCount)
            {
                return xCount - yCount;
            }

            for (int i = 0; i < xCount; i++)
            {
                var xVal = x.GetAt(i);
                var yVal = y.GetAt(i);
                int compare = CompareTo(xVal, yVal);
                if (compare != 0)
                {
                    return compare;
                }
            }

            return 0;
        }
    }
}
