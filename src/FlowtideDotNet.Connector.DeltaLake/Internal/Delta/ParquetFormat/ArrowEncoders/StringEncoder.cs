using Apache.Arrow;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders
{
    class StringEncoder : IArrowEncoder
    {
        private StringArray? _array;

        public bool IsPartitionValueEncoder => false;

        public void AddValue(int index, ref AddToColumnFunc func)
        {
            Debug.Assert(_array != null);

            if (_array.IsNull(index))
            {
                func.AddValue(NullValue.Instance);
                return;
            }

            var memorySlice = _array.ValueBuffer.Memory.Slice(_array.ValueOffsets[index], _array.GetValueLength(index));
            var val = _array.GetBytes(index, out var isNull);
            func.AddValue(new StringValue(memorySlice));
        }

        public void NewBatch(IArrowArray arrowArray)
        {
            if (arrowArray is StringArray stringArray)
            {
                _array = stringArray;
            }
            else
            {
                throw new ArgumentException("Expected string array", nameof(arrowArray));
            }
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
        }
    }
}
