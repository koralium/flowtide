using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base.Engine
{
    public interface ICheckFailureListener
    {
        /// <summary>
        /// Called when a check fails
        /// </summary>
        /// <param name="notification">The notification</param>
        void OnCheckFailure(ref readonly CheckFailureNotification notification);
    }
}
