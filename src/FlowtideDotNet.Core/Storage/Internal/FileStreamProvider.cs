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

using Tenray.ZoneTree.AbstractFileStream;

namespace FlowtideDotNet.Core.Storage.Internal
{
    internal class FileStreamProvider : IFileStreamProvider
    {
        private Dictionary<string, List<LocalFileStream>> openStreams = new Dictionary<string, List<LocalFileStream>>();
        public void CreateDirectory(string path)
        {
            Directory.CreateDirectory(path);
        }

        public IFileStream CreateFileStream(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize = 4096, FileOptions options = FileOptions.None)
        {
            var fileStream = new LocalFileStream(path, mode, access, share, bufferSize, options);
            lock (openStreams)
            {
                if (!openStreams.TryGetValue(path, out var list))
                {
                    list = new List<LocalFileStream>();
                    openStreams.Add(path, list);
                }
                list.Add(fileStream);
            }
            return fileStream;
        }

        public void DeleteDirectory(string path, bool recursive)
        {
            Directory.Delete(path, recursive);
        }

        public void DeleteFile(string path)
        {
            File.Delete(path);
        }

        public bool DirectoryExists(string path)
        {
            return Directory.Exists(path);
        }

        public bool FileExists(string path)
        {
            return File.Exists(path);
        }

        public DurableFileWriter GetDurableFileWriter()
        {
            return new DurableFileWriter(this);
        }

        public byte[] ReadAllBytes(string path)
        {
            return File.ReadAllBytes(path);
        }

        public string ReadAllText(string path)
        {
            return File.ReadAllText(path);
        }

        public void Replace(string sourceFileName, string destinationFileName, string destinationBackupFileName)
        {
            try
            {
                File.Replace(sourceFileName, destinationFileName, destinationBackupFileName);
            }
            catch (IOException ex)
            {
                File.Replace(sourceFileName, destinationFileName, destinationBackupFileName);
            }
            
        }
    }
}
