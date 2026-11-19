using Azure.Storage.Blobs;
using System;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexOutput semantics for a file written to the local cache. The file is
    /// uploaded to blob storage when Lucene syncs it (<see cref="AzureDirectory.Sync"/>), not
    /// when it is closed: files Lucene deletes before syncing them, such as the parts of a segment
    /// packed into a compound file, are never uploaded at all.
    /// </summary>
    public class AzureIndexOutput : IndexOutput
    {
        private readonly AzureDirectory _azureDirectory;
        private readonly string _name;
        private IndexOutput _indexOutput;

        public AzureIndexOutput(AzureDirectory azureDirectory, string name, BlobClient blob)
        {
            _azureDirectory = azureDirectory;
            _name = name;
            using (_azureDirectory.LockFile(name))
            {
                // create the local cache one we will operate against...
                _indexOutput = CacheDirectory.CreateOutput(_name, IOContext.DEFAULT);
            }
        }

        public Lucene.Net.Store.Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }

        public override void Flush()
        {
            _indexOutput?.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            if (_indexOutput == null)
                return;
            _indexOutput.Flush();
            _indexOutput.Dispose();
            _indexOutput = null;
            // Written locally; uploaded if and when Lucene syncs it.
            _azureDirectory.MarkPending(_name);
            GC.SuppressFinalize(this);
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

        [Obsolete("(4.1) this method will be removed in Lucene 5.0")]
        public override void Seek(long pos)
        {
            // obsolete
        }

        public override long Checksum => _indexOutput.Checksum;

        public override long Position => _indexOutput.Position;
    }
}
