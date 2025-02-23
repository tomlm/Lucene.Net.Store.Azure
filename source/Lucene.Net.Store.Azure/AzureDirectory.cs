﻿//    License: Microsoft Public License (Ms-PL) 
using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : BaseDirectory
    {
        private BlobServiceClient _blobClient;
        private readonly string containerName;
        private readonly string subDirectory;

        private readonly Dictionary<string, AzureIndexOutput> _nameCache = new Dictionary<string, AzureIndexOutput>();

        public AzureDirectory(string storageAccount) :
            this(storageAccount, null, null)
        {
        }

        /// <summary>
        /// Create AzureDirectory
        /// </summary>
        /// <param name="storageAccount">staorage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage, can have subfolders like foo/bar)</param>
        /// <remarks>Default local cache is to use file system in user/appdata/AzureDirectory/Catalog</remarks>
        public AzureDirectory(
            string storageAccount,
            string catalog)
            : this(storageAccount, catalog, null)
        {
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage, can have subfolders like foo/bar)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        public AzureDirectory(
            string storageAccount,
            string catalog,
            Directory cacheDirectory) 
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            if (string.IsNullOrEmpty(catalog))
                Name = "lucene";
            else
                Name = catalog.ToLower();

            this.containerName = Name.Split('/').First();
            this.subDirectory = String.Join("/", Name.Split('/').Skip(1));

            _blobClient = new BlobServiceClient(storageAccount);
            _initCacheDirectory(cacheDirectory);
            
            // default lock factory is AzureLockFactory
            SetLockFactory(new AzureLockFactory(this));
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="blobServiceClient">BlobServiceClient to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage, can have subfolders like foo/bar)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        public AzureDirectory(
            BlobServiceClient blobServiceClient,
            string catalog,
            Directory cacheDirectory)
        {
            if (blobServiceClient == null)
                throw new ArgumentNullException("blobServiceClient");

            if (string.IsNullOrEmpty(catalog))
                Name = "lucene";
            else
                Name = catalog.ToLower();

            this.containerName = Name.Split('/').First();
            this.subDirectory = String.Join("/", Name.Split('/').Skip(1));

            _blobClient = blobServiceClient;
            _initCacheDirectory(cacheDirectory);

            // default lock factory is AzureLockFactory
            SetLockFactory(new AzureLockFactory(this));
        }

        public BlobContainerClient BlobContainer { get; private set; }

        public string Name { get; set; }

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
            var prefix = string.IsNullOrEmpty(this.subDirectory) ? null : this.subDirectory + "/";
            
            return BlobContainer.GetBlobsByHierarchy(delimiter: "/", prefix: prefix)
                .Where(x => x.IsBlob)
                .Select(x => x.Blob.Name.Split('/').Last())
                .ToArray();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
            // this always comes from the server
            try
            {
                return BlobContainer.GetBlobClient(GetBlobName(name)).Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name)
        {
            var blobName = GetBlobName(name);
            var blob = BlobContainer.GetBlobClient(blobName);
            blob.DeleteIfExists();
            
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name)
        {
            try
            {
                var blobName = GetBlobName(name);
                return BlobContainer.GetBlobClient(blobName).GetProperties().Value?.ContentLength ?? 0;
            }
            catch
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
                var blobName = GetBlobName(name);
                var blob = BlobContainer.GetBlobClient(blobName);
                return new AzureIndexInput(this, name, blob);
            }
            catch (FileNotFoundException fileNotFoundErr)
            {
                throw;
            }
            catch (Exception err)
            {
                throw new FileNotFoundException(name, err);
            }
        }


        /// <summary>Closes the store. </summary>
        protected override void Dispose(bool disposing)
        {
            BlobContainer = null;
            _blobClient = null;
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            // TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
            var blobName = GetBlobName(name);
            var blob = BlobContainer.GetBlobClient(blobName);
            var indexOutput = new AzureIndexOutput(this, name, blob);
            _nameCache[name] = indexOutput;
            return indexOutput;
        }
        #endregion

        #region internal methods

        public string GetBlobName(string name)
        {
            if (this.subDirectory.Length > 1)
            {
                return $"{subDirectory}/{name}";
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

                string catalogPath = System.IO.Path.Combine(cachePath, this.Name);

                System.IO.DirectoryInfo catalogDir = new System.IO.DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                this.CacheDirectory = FSDirectory.Open(catalogPath);
            }

            CreateContainer();
        }

        public void CreateContainer()
        {
            this.BlobContainer = _blobClient.GetBlobContainerClient(this.containerName);
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
