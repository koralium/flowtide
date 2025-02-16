using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal class EmptyDeleteVector : IDeleteVector
    {
        public static readonly EmptyDeleteVector Instance = new EmptyDeleteVector();

        public bool Contains(long index)
        {
            return false;
        }
    }
}
