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

using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("FlowtideDotNet.Core.Tests")]
[assembly: InternalsVisibleTo("FlowtideDotNet.Connector.OpenFGA")]
[assembly: InternalsVisibleTo("FlowtideDotNet.Connector.SpiceDB")]
[assembly: InternalsVisibleTo("FlowtideDotNet.Connector.Permify")]
[assembly: InternalsVisibleTo("FlowtideDotNet.Connector.Sharepoint")]
[assembly: InternalsVisibleTo("FlowtideDotNet.Benchmarks")]
[assembly: InternalsVisibleTo("SqlSampleWithUI")]
// added only for the purpose of debugging
[assembly: InternalsVisibleTo("FlowtideDotNet.StateDiagnostics")]
[assembly: InternalsVisibleTo("FlowtideDotNet.ComputeTests")]
[assembly: InternalsVisibleTo("FlowtideDotNet.AcceptanceTests")]
[assembly: InternalsVisibleTo("FlowtideDotNet.TestFramework")]