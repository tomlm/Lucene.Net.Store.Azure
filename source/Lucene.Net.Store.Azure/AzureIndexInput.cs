//    License: Microsoft Public License (Ms-PL) 
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;


namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput
    {
        private string _name;
        private AzureDirectory _azureDirectory;
        private BlobContainerClient _blobContainer;
        private BlobClient _blob;
        private IndexInput _indexInput;
        private Mutex _fileMutex;

        public AzureIndexInput(AzureDirectory azureDirectory, string name, BlobClient blob)
            : base(name)
        {
            this._name = name;
            this._azureDirectory = azureDirectory;
#if FULLDEBUG
            Debug.WriteLine($"{_azureDirectory.Name} opening {name} ");
#endif
            _fileMutex = BlobMutexManager.GrabMutex(name);
            _fileMutex.WaitOne();
            try
            {
                _blobContainer = azureDirectory.BlobContainer;
                _blob = blob;
                bool fileNeeded = false;
                if (!CacheDirectory.FileExists(name))
                {
                    fileNeeded = true;
                }
                else
                {
                    try
                    {
                        var blobProperties = blob.GetProperties();
                        long cachedLength = CacheDirectory.FileLength(name);
                        long blobLength = blobProperties?.Value?.ContentLength ?? 0;
                        if (cachedLength != blobLength)
                            fileNeeded = true;
                    }
                    catch (RequestFailedException err)
                    {
                        // if blob not found
                        if (err.Status == 404)
                        {
                            // then we should remove from cache directory.
                            CacheDirectory.DeleteFile(name);
                            Debug.WriteLine($"{_azureDirectory.Name} {name} Does not exist");
                            throw new FileNotFoundException(name, err);
                        }
                    }
                }

                // if the file does not exist
                // or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
                if (fileNeeded)
                {
                    try
                    {
                        using (StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(name))
                        {

                            // get the blob
                            _blob.DownloadTo(fileStream);
                            fileStream.Flush();

                            Debug.WriteLine($"{_azureDirectory.Name} GET {_name} RETREIVED {fileStream.Length} bytes");
                        }
                    }
                    catch (RequestFailedException err)
                    {
                        // if blob not found
                        if (err.Status == 404)
                        {
                            // then we should remove from cache directory.
                            CacheDirectory.DeleteFile(name);
                            Debug.WriteLine($"{_azureDirectory.Name} {name} Does not exist");
                            throw new FileNotFoundException(name, err);
                        }
                    }
                    catch (Exception err)
                    {
                        Debug.WriteLine($"{_azureDirectory.Name} GET {_name} ERROR {err.Message}");
                    }
                }
#if FULLDEBUG
                Debug.WriteLine($"{_azureDirectory.Name} Using cached file for {name}");
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

        public override void Seek(long pos)
        {
            _indexInput?.Seek(pos);
        }

        public override long Length => _indexInput.Length;

        public override long Position => _indexInput.Position;

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
#if FULLDEBUG
                Debug.WriteLine($"{_azureDirectory.Name} CLOSED READSTREAM local {_name}");
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
            clone.Seek(this.Position);
            return clone;
        }
    }
}