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

using BenchmarkDotNet.Running;
using FlowtideDotNet.Nexmark;

//Query3 query3 = new Query3();
//query3.Setup();

//for (int i = 0; i < 10000; i++)
//{
//    query3.IterationSetup();
//    await query3.Q3();
//}


var summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args: [.. args, "--join"]);