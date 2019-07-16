//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Lucene.Net.Store;
using Microsoft.Azure.Storage.Blob;


namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput
    {
        private string _name;
        private AzureDirectory _azureDirectory;
        private CloudBlobContainer _blobContainer;
        private CloudBlob _blob;
        private IndexInput _indexInput;
        private Mutex _fileMutex;

        public AzureIndexInput(AzureDirectory azureDirectory, string name, CloudBlob blob)
            : base(name)
        {
            this._name = name;
#if FULLDEBUG
            Debug.WriteLine(String.Format("opening {0} ", name));
#endif
            _fileMutex = BlobMutexManager.GrabMutex(name);
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azureDirectory;
                _blobContainer = azureDirectory.BlobContainer;
                _blob = blob;

                bool fileNeeded = false;
                if (!CacheDirectory.FileExists(name))
                {
                    fileNeeded = true;
                }
                else
                {
                    long cachedLength = CacheDirectory.FileLength(name);
                    long blobLength = blob.Properties.Length;
                    if (cachedLength != blobLength)
                        fileNeeded = true;
                }

                // if the file does not exist
                // or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
                if (fileNeeded)
                {
                    using (StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(name))
                    {
                        // get the blob
                        _blob.DownloadToStream(fileStream);
                        fileStream.Flush();

                        Debug.WriteLine(string.Format("GET {0} RETREIVED {1} bytes", _name, fileStream.Length));
                    }
                }
#if FULLDEBUG
                Debug.WriteLine(String.Format("Using cached file for {0}", name));
#endif
                // and open it as our input, this is now available forevers until new file comes along
                _indexInput = CacheDirectory.OpenInput(name, IOContext.DEFAULT);

            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public Lucene.Net.Store.Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }

        public override byte ReadByte()
        {
            return _indexInput.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            _indexInput.ReadBytes(b, offset, len);
        }

        public override long GetFilePointer()
        {
            return _indexInput.GetFilePointer();
        }

        public override void Seek(long pos)
        {
            _indexInput?.Seek(pos);
        }

        public override long Length => _indexInput.Length;

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
#if FULLDEBUG
                Debug.WriteLine(String.Format("CLOSED READSTREAM local {0}", _name));
#endif
                _indexInput.Dispose();
                _indexInput = null;
                _azureDirectory = null;
                _blobContainer = null;
                _blob = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override Object Clone()
        {
            var clone = new AzureIndexInput(this._azureDirectory, this._name, this._blob);
            clone.Seek(this.GetFilePointer());
            return clone;
        }
    }
}