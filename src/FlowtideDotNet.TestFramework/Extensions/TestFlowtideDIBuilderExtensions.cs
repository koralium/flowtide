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

using FlowtideDotNet.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.TestFramework
{
    public static class TestFlowtideDIBuilderExtensions
    {
        public static IFlowtideDIBuilder AddStreamTestMonitor(this IFlowtideDIBuilder builder, StreamTestMonitor monitor)
        {
            builder.AddCustomOptions((provider, b) =>
            {
                b.WithCheckpointListener(monitor);
                b.WithFailureListener(monitor);
            });
            return builder;
        }
    }
}
