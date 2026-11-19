using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lucene.Net.Store.Azure.Tests
{
    /// <summary>
    /// Files reach blob storage when Lucene syncs them, and the per-file locks are scoped to the
    /// local cache.
    /// </summary>
    [TestClass]
    public class SyncAndLockTests
    {
        private const string ConnectionString = "UseDevelopmentStorage=true";

        private static string NewCatalog(string test) =>
            $"syncandlocktests/{test.ToLowerInvariant()}-{Guid.NewGuid():N}";

        private static string NewCacheFolder()
        {
            var folder = Path.Combine(Path.GetTempPath(), "AzureDirectoryTests", Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(folder);
            return folder;
        }

        private static AzureDirectory Open(string catalog, string cacheFolder = null, Uploads uploads = null)
        {
            var options = new BlobClientOptions();
            if (uploads != null)
                options.AddPolicy(uploads, HttpPipelinePosition.PerCall);
            return new AzureDirectory(new BlobServiceClient(ConnectionString, options), catalog, FSDirectory.Open(cacheFolder ?? NewCacheFolder()));
        }

        /// <summary>Notes the name of every file uploaded (a blob PUT that is not a lease call).</summary>
        private sealed class Uploads : HttpPipelineSynchronousPolicy
        {
            public ConcurrentQueue<string> Names { get; } = new ConcurrentQueue<string>();

            public override void OnSendingRequest(HttpMessage message)
            {
                var uri = message.Request.Uri.ToUri();
                if (message.Request.Method == RequestMethod.Put && !uri.Query.Contains("comp=") && !uri.Query.Contains("restype="))
                    Names.Enqueue(uri.AbsolutePath.Substring(uri.AbsolutePath.LastIndexOf('/') + 1));
            }
        }

        private static IndexWriter NewWriter(Directory directory) =>
            new IndexWriter(directory, new IndexWriterConfig(LuceneVersion.LUCENE_48, new StandardAnalyzer(LuceneVersion.LUCENE_48)));

        private static void AddDocuments(IndexWriter writer, int count)
        {
            for (var i = 0; i < count; i++)
                writer.AddDocument(new Document
                {
                    new StringField("id", i.ToString(), Field.Store.YES),
                    new TextField("body", $"the quick brown fox number {i}", Field.Store.YES),
                });
        }

        /// <summary>The blobs under the directory's folder, by file name.</summary>
        private static string[] BlobsOf(AzureDirectory directory) =>
            directory.BlobContainer.GetBlobs(prefix: directory.GetBlobName(""))
                .Select(b => b.Name.Substring(b.Name.LastIndexOf('/') + 1))
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToArray();

        [TestMethod]
        public void ACommit_UploadsOnlyTheFilesItNeeds()
        {
            var uploads = new Uploads();
            var directory = Open(NewCatalog(nameof(ACommit_UploadsOnlyTheFilesItNeeds)), uploads: uploads);
            using (var writer = NewWriter(directory))
            {
                AddDocuments(writer, 50);
                writer.Commit();

                // Uploaded, not merely left behind: before, every file was uploaded on close and the
                // parts of the compound file were deleted again afterwards.
                var uploaded = uploads.Names.ToArray();

                var commit = new SegmentInfos();
                commit.Read(directory);
                var needed = commit.GetFiles(directory, true).ToHashSet(StringComparer.Ordinal);
                var blobs = BlobsOf(directory);

                // Everything the commit needs is there...
                CollectionAssert.IsSubsetOf(needed.ToArray(), blobs);
                // ...and nothing else: not the parts of the segment packed into its compound file.
                var extra = uploaded.Where(b => !needed.Contains(b) && b != "write.lock" && b != IndexFileNames.SEGMENTS_GEN).ToArray();
                Assert.AreEqual(0, extra.Length, "Uploaded but not part of the commit: " + string.Join(", ", extra));
                Assert.AreEqual(uploaded.Length, uploaded.Distinct().Count(), "A file was uploaded twice: " + string.Join(", ", uploaded));
            }
        }

        [TestMethod]
        public void AWriterThatNeverCommits_UploadsNoSegmentFiles()
        {
            var uploads = new Uploads();
            var directory = Open(NewCatalog(nameof(AWriterThatNeverCommits_UploadsNoSegmentFiles)), uploads: uploads);
            var writer = NewWriter(directory);
            AddDocuments(writer, 50);
            writer.Flush(triggerMerge: false, applyAllDeletes: true);
            writer.Rollback();

            var segmentFiles = uploads.Names.Where(n => n.StartsWith("_", StringComparison.Ordinal)).ToArray();
            Assert.AreEqual(0, segmentFiles.Length, "Segment files were uploaded: " + string.Join(", ", segmentFiles));
        }

        [TestMethod]
        public void WhatOneDirectoryCommits_AnotherWithItsOwnCacheReads()
        {
            var catalog = NewCatalog(nameof(WhatOneDirectoryCommits_AnotherWithItsOwnCacheReads));
            var writing = Open(catalog);
            using (var writer = NewWriter(writing))
            {
                AddDocuments(writer, 50);
                writer.Commit();
                AddDocuments(writer, 25);
                writer.Commit();
            }

            var reading = Open(catalog);
            using (var reader = DirectoryReader.Open(reading))
                Assert.AreEqual(75, reader.NumDocs);
        }

        [TestMethod]
        public void AWriterReadsWhatItHasNotYetUploaded()
        {
            var directory = Open(NewCatalog(nameof(AWriterReadsWhatItHasNotYetUploaded)));
            using (var writer = NewWriter(directory))
            {
                AddDocuments(writer, 50);
                // A near-real-time reader opens the flushed segment before any commit uploads it.
                using (var reader = DirectoryReader.Open(writer, applyAllDeletes: true))
                    Assert.AreEqual(50, reader.NumDocs);
                writer.Commit();
            }
        }

        [TestMethod]
        public void FileLocks_AreScopedToTheCache_NotTheFileName()
        {
            var catalog = NewCatalog(nameof(FileLocks_AreScopedToTheCache_NotTheFileName));
            var sharedCache = NewCacheFolder();
            var first = Open(catalog, sharedCache);
            var ownCache = Open(catalog);
            var sameCache = Open(catalog, sharedCache);

            using var held = new ManualResetEventSlim();
            using var done = new ManualResetEventSlim();
            // A mutex belongs to the thread that took it, so it is held on a thread of its own.
            var holder = new Thread(() =>
            {
                using (first.LockFile("segments.gen"))
                {
                    held.Set();
                    done.Wait();
                }
            });
            holder.Start();
            held.Wait();
            try
            {
                // A directory with a cache of its own does not wait.
                var other = Task.Run(() => { using (ownCache.LockFile("segments.gen")) { } });
                Assert.IsTrue(other.Wait(TimeSpan.FromSeconds(5)), "A directory with its own cache waited for another's file lock.");

                // One sharing the cache folder does.
                var shared = Task.Run(() => { using (sameCache.LockFile("segments.gen")) { } });
                Assert.IsFalse(shared.Wait(TimeSpan.FromMilliseconds(500)), "A directory sharing the cache did not wait for the file lock.");
                done.Set();
                Assert.IsTrue(shared.Wait(TimeSpan.FromSeconds(5)));
            }
            finally
            {
                done.Set();
                holder.Join();
            }
        }
    }
}
