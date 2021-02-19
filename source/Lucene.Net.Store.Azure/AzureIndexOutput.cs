using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexOutput semantics for a write/append straight to blob storage
    /// </summary>
    public class AzureIndexOutput : IndexOutput
    {
        private AzureDirectory _azureDirectory;
        private CloudBlobContainer _blobContainer;
        private string _name;
        private IndexOutput _indexOutput;
        private Mutex _fileMutex;
        private ICloudBlob _blob;

        public AzureIndexOutput(AzureDirectory azureDirectory, string name, CloudBlockBlob blob)
        {
            this._name = name;
            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azureDirectory;
                _blobContainer = _azureDirectory.BlobContainer;
                _blob = blob;

                // create the local cache one we will operate against...
                _indexOutput = CacheDirectory.CreateOutput(_name, IOContext.DEFAULT);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public Lucene.Net.Store.Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }

        public override void Flush()
        {
            _indexOutput?.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
                // make sure it's all written out
                _indexOutput.Flush();

                long originalLength = _indexOutput.Length;
                _indexOutput.Dispose();

                using (var blobStream = new StreamInput(CacheDirectory.OpenInput(_name, IOContext.DEFAULT)))
                {
                    // push the blobStream up to the cloud
                    _blob.UploadFromStream(blobStream);

                    // set the metadata with the original index file properties
                    _blob.SetMetadata();

                    Debug.WriteLine($"{_azureDirectory.Name} PUT {_name} bytes to {blobStream.Length} in cloud");
                }

#if FULLDEBUG
                Debug.WriteLine($"{_azureDirectory.Name} CLOSED WRITESTREAM {_name}");
#endif
                // clean up
                _indexOutput = null;
                _blobContainer = null;
                _blob = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override long Length => _indexOutput.Length;

        public override void WriteByte(byte b)
        {
            _indexOutput.WriteByte(b);
        }

        public override void WriteBytes(byte[] b, int length)
        {
            _indexOutput.WriteBytes(b, length);
        }

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            _indexOutput.WriteBytes(b, offset, length);
        }

        public override long GetFilePointer()
        {
            return _indexOutput.GetFilePointer();
        }

        public override void Seek(long pos)
        {
            //_indexOutput.Seek(pos);
        }

        public override long Checksum => _indexOutput.Checksum;
    }
}
