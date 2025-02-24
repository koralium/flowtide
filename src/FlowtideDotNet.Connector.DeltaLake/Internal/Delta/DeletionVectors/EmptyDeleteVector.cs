using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal class EmptyDeleteVector : IDeleteVector
    {
        public static readonly EmptyDeleteVector Instance = new EmptyDeleteVector();

        public long Cardinality => 0;

        public bool Contains(long index)
        {
            return false;
        }

        private IEnumerable<long> Empty()
        {
            yield break;
        }

        public IEnumerator<long> GetEnumerator()
        {
            return Empty().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Empty().GetEnumerator();
        }
    }
}
