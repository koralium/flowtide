using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal interface IDeleteVector : IEnumerable<long>
    {
        bool Contains(long index);

        long Cardinality { get; }
    }
}
