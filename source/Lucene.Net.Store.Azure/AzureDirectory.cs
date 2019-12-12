//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;
using System.IO;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : Directory
    {
        private string _catalog;
        private string _subDirectory;
        private CloudBlobClient _blobClient;

        private readonly Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();
        private LockFactory _lockFactory = new NativeFSLockFactory();
        private readonly Dictionary<string, AzureIndexOutput> _nameCache = new Dictionary<string, AzureIndexOutput>();

        public override LockFactory LockFactory => _lockFactory;

        public AzureDirectory(CloudStorageAccount storageAccount) :
            this(storageAccount, null, null)
        {
        }

        /// <summary>
        /// Create AzureDirectory
        /// </summary>
        /// <param name="storageAccount">staorage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <remarks>Default local cache is to use file system in user/appdata/AzureDirectory/Catalog</remarks>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog)
            : this(storageAccount, catalog, null)
        {
        }

        /// <summary>
        /// Create AzureDirectory
        /// </summary>
        /// <param name="storageAccount">staorage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <param name="subDirectory">name of subdirectory within the catalog</param>
        /// <remarks>Default local cache is to use file system in user/appdata/AzureDirectory/Catalog</remarks>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog,
            string subDirectory)
            : this(storageAccount, catalog, subDirectory, null)
        {
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog,
            string subDirectory,
            Directory cacheDirectory)
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            if (string.IsNullOrEmpty(catalog))
                _catalog = "lucene";
            else
                _catalog = catalog.ToLower();

            if (!string.IsNullOrWhiteSpace(subDirectory))
            {
                _subDirectory = subDirectory.ToLower();
            }

            _blobClient = storageAccount.CreateCloudBlobClient();
            _initCacheDirectory(cacheDirectory);
        }


        public CloudBlobContainer BlobContainer { get; private set; }

        /// <summary>
        /// If set, this is the directory object to use as the local cache
        /// </summary>
        public Directory CacheDirectory { get; set; }

        public void ClearCache()
        {
            if (this.CacheDirectory != null)
            {
                foreach (string file in CacheDirectory.ListAll())
                {
                    CacheDirectory.DeleteFile(file);
                }
            }
        }
        #region DIRECTORYMETHODS

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override string[] ListAll()
        {
            var results = Enumerable.Empty<string>();
            if (string.IsNullOrWhiteSpace(_subDirectory))
            {
                results = from blob in BlobContainer.ListBlobs()
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            }
            else
            {
                results = from blob in BlobContainer.ListBlobs().Where(blob => blob.Uri.AbsolutePath.Contains($"/{_subDirectory}/"))
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            }
            return results.ToArray();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
            // this always comes from the server
            try
            {
                var nameDecorated = DecorateName(name);
                return BlobContainer.GetBlockBlobReference(nameDecorated).Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name)
        {
            var nameDecorated = DecorateName(name);
            var blob = BlobContainer.GetBlockBlobReference(nameDecorated);
            blob.DeleteIfExists();
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name)
        {
            try
            {
                var nameDecorated = DecorateName(name);
                var blob = BlobContainer.GetBlockBlobReference(nameDecorated);
                blob.FetchAttributes();
                return blob.Properties.Length;
            }
            catch (Exception err)
            {
                return 0;
            }
        }

        public override void Sync(ICollection<string> names)
        {
            // TODO: This all is purely guesswork, no idea what has to be done here. -- Aviad.
            foreach (var name in names)
            {
                if (_nameCache.ContainsKey(name))
                {
                    _nameCache[name].Flush();
                }
            }
        }

        public override IndexInput OpenInput(string name, IOContext context)
        {
            // TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
            try
            {
                var nameDecorated = DecorateName(name);
                var blob = BlobContainer.GetBlockBlobReference(nameDecorated);
                blob.FetchAttributes();
                return new AzureIndexInput(this, name, blob);
            }
            catch (Exception err)
            {
                throw new FileNotFoundException(name, err);
            }
        }

        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock MakeLock(string name)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(name))
                {
                    _locks.Add(name, new AzureLock(name, this));
                }
                return _locks[name];
            }
        }

        public override void ClearLock(string name)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(name))
                {
                    _locks[name].BreakLock();
                }
            }
        }

        /// <summary>Closes the store. </summary>
        protected override void Dispose(bool disposing)
        {
            BlobContainer = null;
            _blobClient = null;
        }

        public override void SetLockFactory(LockFactory lockFactory)
        {
            _lockFactory = lockFactory;
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            // TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
            var decoratedName = DecorateName(name);
            var blob = BlobContainer.GetBlockBlobReference(decoratedName);
            var indexOutput = new AzureIndexOutput(this, name, blob);
            _nameCache[name] = indexOutput;
            return indexOutput;
        }
        #endregion

        #region internal methods

        private string DecorateName(string name)
        {
            if (!string.IsNullOrWhiteSpace(_subDirectory))
            {
                return $"{ _subDirectory}/{name}";
            }

            return name;
        }

        private void _initCacheDirectory(Directory cacheDirectory)
        {
            if (cacheDirectory != null)
            {
                // save it off
                this.CacheDirectory = cacheDirectory;
            }
            else
            {
                string cachePath = System.IO.Path.Combine(Environment.ExpandEnvironmentVariables("%temp%"), "AzureDirectory");
                System.IO.DirectoryInfo azureDir = new System.IO.DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                string catalogPath = System.IO.Path.Combine(cachePath, _catalog);
                if (!string.IsNullOrWhiteSpace(_subDirectory))
                {
                    catalogPath = System.IO.Path.Combine(cachePath, _catalog, _subDirectory);
                }

                System.IO.DirectoryInfo catalogDir = new System.IO.DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                this.CacheDirectory = FSDirectory.Open(catalogPath);
            }

            CreateContainer();
        }

        public void CreateContainer()
        {
            this.BlobContainer = _blobClient.GetContainerReference(_catalog);
            this.BlobContainer.CreateIfNotExists();
        }

        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(this.CacheDirectory.OpenInput(name, IOContext.DEFAULT));
        }

        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(this.CacheDirectory.CreateOutput(name, IOContext.DEFAULT));
        }


        #endregion
    }
}
