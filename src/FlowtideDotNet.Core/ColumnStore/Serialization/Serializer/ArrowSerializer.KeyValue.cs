using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="keyOffset">Reference to a string</param>
        /// <param name="valueOffset">Reference to a string</param>
        /// <returns></returns>
        public int CreateKeyValue(
          int keyOffset = 0,
          int valueOffset = 0)
        {
            StartTable(2);
            KeyValueAddValue(valueOffset);
            KeyValueAddKey(keyOffset);
            return EndTable();
        }

        private void KeyValueAddKey(int keyOffset) 
        { 
            AddOffset(0, keyOffset, 0); 
        }
        public void KeyValueAddValue(int valueOffset) 
        { 
            AddOffset(1, valueOffset, 0); 
        }
    }
}
