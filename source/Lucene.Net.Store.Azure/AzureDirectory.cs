//    License: Microsoft Public License (Ms-PL) 
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : BaseDirectory
    {
        private BlobServiceClient _blobClient;
        private readonly string containerName;
        private readonly string subDirectory;

        // Blob metadata cache — refreshed by ListAll(); lazily populated on first access
        private Dictionary<string, BlobMetadata> _blobMetadataCache;
        private readonly object _cacheLock = new object();

        // Files written to the local cache and closed, but not yet uploaded: Lucene has not synced
        // them. They are uploaded by Sync, or never, when Lucene deletes them first.
        private readonly HashSet<string> _pending = new HashSet<string>(StringComparer.Ordinal);

        // Local copies of deleted files that could not be removed yet, because a reader still had
        // them open (Windows will not delete an open file). Tried again on later deletes and syncs.
        private readonly HashSet<string> _staleCopies = new HashSet<string>(StringComparer.Ordinal);

        // How many locks of this directory this process holds. While it holds one (the index
        // writer's), no other process can change the directory, so after one fresh listing the
        // metadata cache is kept up to date by this process's own writes and deletes.
        private int _locksHeld;
        private bool _listedWhileLocked;

        // What the per-file locks are scoped to: the local cache, so two directories (or two
        // processes) sharing a cache folder take the same lock for the same file, and nothing else does.
        private string _lockScope;

        // How many files Sync uploads at once.
        private const int MaxParallelUploads = 8;

        /// <summary>
        /// Create AzureDirectory with just storagaccount string
        /// </summary>
        /// <param name="storageAccount"></param>
        public AzureDirectory(string storageAccount) :
            this(storageAccount,
                catalog: null,
                cacheDirectory: null,
                multiCasePath: false)
        {
        }

        /// <summary>
        /// Create AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (container in blobstorage, can have subfolders like foo/bar)</param>
        /// <param name="multiCasePath">allow subdirectories to be multi-case </param>
        /// <remarks>Default local cache is to use file system in user/appdata/AzureDirectory/Catalog</remarks>
        public AzureDirectory(
            string storageAccount,
            string catalog,
            bool multiCasePath = false)
            : this(storageAccount,
                  catalog: catalog,
                  cacheDirectory: null,
                  multiCasePath: multiCasePath)
        {
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (container in blobstorage, can have subfolders like foo/bar)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        /// <param name="multiCasePath">allow subdirectories to be multi-case </param>
        public AzureDirectory(
            string storageAccount,
            string catalog,
            Directory cacheDirectory,
            bool multiCasePath = false)
            : this(new BlobServiceClient(storageAccount),
                  catalog: catalog,
                  cacheDirectory: cacheDirectory,
                  multiCasePath: multiCasePath)
        {
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="blobServiceClient">BlobServiceClient to use</param>
        /// <param name="catalog">name of catalog (container in blobstorage, can have subfolders like foo/bar)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        /// <param name="multiCasePath">allow subdirectories to be multi-case</param>
        public AzureDirectory(
            BlobServiceClient blobServiceClient,
            string catalog,
            Directory cacheDirectory,
            bool multiCasePath = false)
        {
            if (blobServiceClient == null)
                throw new ArgumentNullException("blobServiceClient");

            if (string.IsNullOrEmpty(catalog))
                catalog = "lucene";

            this.containerName = catalog.Split('/').First().ToLower();
            this.subDirectory = String.Join("/", catalog.Split('/').Skip(1));
            if (multiCasePath == false)
                this.subDirectory = this.subDirectory.ToLower();
            this.Name = $"{containerName}/{subDirectory}".TrimEnd('/');

            _blobClient = blobServiceClient;
            _initCacheDirectory(cacheDirectory);

            // default lock factory is AzureLockFactory
            SetLockFactory(new AzureLockFactory(this));
        }

        public BlobContainerClient BlobContainer { get; private set; }

        /// <summary>
        /// Metadata cached for a single blob.
        /// </summary>
        internal struct BlobMetadata
        {
            public long ContentLength;
            public string ETag;
            public DateTimeOffset? LastModified;
        }

        /// <summary>
        /// Refreshes the in-memory blob metadata cache by issuing a single ListBlobs call.
        /// </summary>
        private void RefreshBlobMetadataCache()
        {
            try
            {
                var prefix = string.IsNullOrEmpty(this.subDirectory) ? null : this.subDirectory + "/";
                var newCache = new Dictionary<string, BlobMetadata>(StringComparer.Ordinal);

                foreach (var item in BlobContainer.GetBlobsByHierarchy(delimiter: "/", prefix: prefix, traits: BlobTraits.None, states: BlobStates.None))
                {
                    if (!item.IsBlob)
                        continue;

                    var blobName = item.Blob.Name;
                    var lastSlashIndex = blobName.LastIndexOf('/');
                    var shortName = lastSlashIndex >= 0 ? blobName.Substring(lastSlashIndex + 1) : blobName;
                    var props = item.Blob.Properties;
                    newCache[shortName] = new BlobMetadata
                    {
                        ContentLength = props.ContentLength ?? 0,
                        ETag = props.ETag?.ToString(),
                        LastModified = props.LastModified
                    };
                }

                _blobMetadataCache = newCache;
            }
            catch (RequestFailedException)
            {
            }
        }

        /// <summary>
        /// Ensures the blob metadata cache has been populated at least once.
        /// The authoritative refresh happens in <see cref="ListAll"/>, which Lucene
        /// always calls before opening files. This method only does a full list if
        /// the cache has never been populated.
        /// </summary>
        internal void EnsureCacheUpToDate()
        {
            lock (_cacheLock)
            {
                if (_blobMetadataCache == null)
                    RefreshBlobMetadataCache();
            }
        }

        /// <summary>
        /// Returns the cached <see cref="BlobMetadata"/> for <paramref name="name"/>, or null if not found.
        /// </summary>
        internal BlobMetadata? GetCachedMetadata(string name)
        {
            lock (_cacheLock)
            {
                if (_blobMetadataCache != null && _blobMetadataCache.TryGetValue(name, out var meta))
                    return meta;
                return null;
            }
        }

        /// <summary>
        /// Removes a single entry from the metadata cache (e.g. after delete or write).
        /// </summary>
        private void InvalidateCacheEntry(string name)
        {
            lock (_cacheLock)
            {
                _blobMetadataCache?.Remove(name);
            }
        }

        /// <summary>
        /// Updates (or inserts) the cached metadata for a blob after a successful upload,
        /// avoiding a re-download when the file is immediately read back.
        /// </summary>
        internal void UpdateCacheEntry(string name, long contentLength)
        {
            lock (_cacheLock)
            {
                if (_blobMetadataCache == null)
                    return;

                _blobMetadataCache[name] = new BlobMetadata
                {
                    ContentLength = contentLength
                };
            }
        }

        public string Name { get; private set; }

        /// <summary>
        /// If set, this is the directory object to use as the local cache
        /// </summary>
        public Directory CacheDirectory { get; private set; }

        /// <summary>
        /// Deletes the files in the local cache, except those not yet uploaded: the cache holds
        /// their only copy.
        /// </summary>
        public void ClearCache()
        {
            if (this.CacheDirectory != null)
            {
                foreach (string file in CacheDirectory.ListAll())
                {
                    using (LockFile(file))
                    {
                        if (IsPending(file))
                            continue;
                        CacheDirectory.DeleteFile(file);
                        lock (_cacheLock)
                        {
                            _staleCopies.Remove(file);
                        }
                    }
                }
            }
        }
        #region DIRECTORYMETHODS

        /// <summary>
        /// Returns an array of strings, one for each file in the directory: those in blob storage,
        /// and those written here and not yet uploaded.
        /// </summary>
        public override string[] ListAll()
        {
            lock (_cacheLock)
            {
                // A fresh listing, so other processes' changes are seen — except while this
                // process holds the directory's lock and has listed once since taking it: then
                // nobody else can change the container, and the cache is already current.
                if (_locksHeld == 0 || !_listedWhileLocked || _blobMetadataCache == null)
                {
                    RefreshBlobMetadataCache();
                    _listedWhileLocked = _locksHeld > 0;
                }
                var names = new HashSet<string>(_blobMetadataCache?.Keys ?? Enumerable.Empty<string>(), StringComparer.Ordinal);
                names.UnionWith(_pending);
                return names.ToArray();
            }
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
            if (IsPending(name))
                return true;
            EnsureCacheUpToDate();
            return GetCachedMetadata(name).HasValue;
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name)
        {
            bool wasPending, inBlobStorage;
            lock (_cacheLock)
            {
                wasPending = _pending.Remove(name);
                inBlobStorage = _blobMetadataCache == null || _blobMetadataCache.ContainsKey(name);
            }
            // A file written here and never uploaded has no blob (unless an earlier version was uploaded).
            if (!wasPending || inBlobStorage)
            {
                var blobName = GetBlobName(name);
                var blob = BlobContainer.GetBlobClient(blobName);
                blob.DeleteIfExists();
                InvalidateCacheEntry(name);
            }
            // The local copy goes too, or the cache would grow without bound.
            DeleteLocalCopy(name, retry: false);
            DeleteStaleCopies();
        }

        /// <summary>
        /// Removes a deleted file's copy from the local cache. A copy a reader still has open is
        /// remembered and removed on a later delete or sync.
        /// </summary>
        private void DeleteLocalCopy(string name, bool retry)
        {
            // All under the file's lock, which CreateOutput also takes, so a file written again
            // since it was deleted is never taken for the old copy.
            using (LockFile(name))
            {
                if (retry)
                {
                    lock (_cacheLock)
                    {
                        // Written again since the retry was picked: the copy is the new file.
                        if (!_staleCopies.Contains(name) || _pending.Contains(name))
                            return;
                    }
                }
                bool deleted;
                try
                {
                    CacheDirectory.DeleteFile(name);
                    deleted = true;
                }
                catch (FileNotFoundException)
                {
                    deleted = true;
                }
                catch (Exception e) when (e is IOException || e is UnauthorizedAccessException)
                {
                    deleted = false;
                }
                lock (_cacheLock)
                {
                    if (deleted)
                        _staleCopies.Remove(name);
                    else
                        _staleCopies.Add(name);
                }
            }
        }

        /// <summary>Tries again to remove the local copies that could not be removed before.</summary>
        private void DeleteStaleCopies()
        {
            string[] stale;
            lock (_cacheLock)
            {
                if (_staleCopies.Count == 0)
                    return;
                stale = _staleCopies.ToArray();
            }
            foreach (var name in stale)
                DeleteLocalCopy(name, retry: true);
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name)
        {
            if (IsPending(name))
                return CacheDirectory.FileLength(name);
            EnsureCacheUpToDate();
            return GetCachedMetadata(name)?.ContentLength ?? 0;
        }

        /// <summary>
        /// Makes the files durable: uploads those written here and not yet uploaded. Lucene syncs
        /// every file a commit needs before it writes the commit, so a file it never syncs (one it
        /// deletes first) never reaches blob storage.
        /// </summary>
        public override void Sync(ICollection<string> names)
        {
            string[] toUpload;
            lock (_cacheLock)
            {
                toUpload = names.Where(_pending.Contains).Distinct(StringComparer.Ordinal).ToArray();
            }
            if (toUpload.Length > 0)
                Parallel.ForEach(toUpload, new ParallelOptions { MaxDegreeOfParallelism = MaxParallelUploads }, Upload);
            DeleteStaleCopies();
        }

        /// <summary>Uploads a file from the local cache to blob storage.</summary>
        private void Upload(string name)
        {
            using (LockFile(name))
            {
                lock (_cacheLock)
                {
                    // Deleted, or uploaded by another Sync, since it was picked.
                    if (!_pending.Contains(name))
                        return;
                }
                long length;
                using (var stream = new StreamInput(CacheDirectory.OpenInput(name, IOContext.DEFAULT)))
                {
                    length = stream.Length;
                    BlobContainer.GetBlobClient(GetBlobName(name)).Upload(stream, overwrite: true);
                }
                lock (_cacheLock)
                {
                    _pending.Remove(name);
                }
                UpdateCacheEntry(name, length);
            }
        }

        /// <summary>A file written here and closed, waiting for Lucene to sync it.</summary>
        internal void MarkPending(string name)
        {
            lock (_cacheLock)
            {
                _pending.Add(name);
            }
        }

        /// <summary>Whether a file was written here and not yet uploaded, so the local cache holds the only copy.</summary>
        internal bool IsPending(string name)
        {
            lock (_cacheLock)
            {
                return _pending.Contains(name);
            }
        }

        /// <summary>Called by <see cref="AzureLock"/> when it takes the directory's lock.</summary>
        internal void OnLockObtained()
        {
            lock (_cacheLock)
            {
                if (_locksHeld++ == 0)
                    _listedWhileLocked = false;
            }
        }

        /// <summary>Called by <see cref="AzureLock"/> when it gives up the directory's lock.</summary>
        internal void OnLockReleased()
        {
            lock (_cacheLock)
            {
                if (_locksHeld > 0)
                    _locksHeld--;
            }
        }

        /// <summary>
        /// Takes the lock for one file of the local cache, so a download, an upload or a new local
        /// copy of it are never made at the same time. It is scoped to the cache, not the machine:
        /// directories with caches of their own never wait for one another.
        /// </summary>
        internal IDisposable LockFile(string name) => new FileLock(_lockScope, name);

        private sealed class FileLock : IDisposable
        {
            private Mutex _mutex;

            public FileLock(string scope, string name)
            {
                // A named mutex, so processes sharing one cache folder are covered too. The name is
                // a hash: mutex names cannot hold a backslash and are limited in length.
                string hash;
                using (var sha = SHA256.Create())
                    hash = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(scope + "|" + name)), 0, 16).Replace("-", "");
                _mutex = new Mutex(false, "LuceneAzureFile_" + hash);
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException)
                {
                    // A process died holding it; the lock is ours now.
                }
            }

            public void Dispose()
            {
                if (_mutex == null)
                    return;
                _mutex.ReleaseMutex();
                _mutex.Dispose();
                _mutex = null;
            }
        }

        public override IndexInput OpenInput(string name, IOContext context)
        {
            // TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
            EnsureCacheUpToDate();
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
            // Under the file's lock, so a retried delete of the old copy comes before it, or not at all.
            using (LockFile(name))
            {
                lock (_cacheLock)
                {
                    // Written again (segments.gen is): its new local copy is not stale.
                    _staleCopies.Remove(name);
                }
                return new AzureIndexOutput(this, name, blob);
            }
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
                string cachePath = Path.Combine(Path.GetTempPath(), "AzureDirectory");
                System.IO.DirectoryInfo azureDir = new System.IO.DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                string catalogPath = System.IO.Path.Combine(cachePath, this.Name);

                System.IO.DirectoryInfo catalogDir = new System.IO.DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                this.CacheDirectory = FSDirectory.Open(catalogPath);
            }

            // A cache in a folder, even wrapped, is scoped to the folder. Any other cache (in memory,
            // or a directory of the caller's own) is taken to be this instance's alone.
            var fsDirectory = FindFSDirectory(this.CacheDirectory);
            _lockScope = fsDirectory != null
                ? Path.GetFullPath(fsDirectory.Directory.FullName).ToLowerInvariant()
                : $"{this.Name}#{Guid.NewGuid():N}";

            CreateContainer();
        }

        /// <summary>The <see cref="FSDirectory"/> a cache is, or wraps; null if none.</summary>
        private static FSDirectory FindFSDirectory(Directory directory)
        {
            while (true)
            {
                switch (directory)
                {
                    case FSDirectory fsDirectory:
                        return fsDirectory;
                    case FilterDirectory filter:
                        directory = filter.Delegate;
                        break;
                    case NRTCachingDirectory nrtCaching:   // not a FilterDirectory in Lucene.Net 4.8
                        directory = nrtCaching.Delegate;
                        break;
                    default:
                        return null;
                }
            }
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
