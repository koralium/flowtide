﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base.Metrics.Internal
{
    internal class FlowtideObservableCounter<T> : IObservableCounter<T>
        where T : struct
    {
        private readonly ObservableCounter<T> m_counter;

        public FlowtideObservableCounter(ObservableCounter<T> counter)
        {
            this.m_counter = counter;
        }
    }
}
