using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lucene.Net.Store.Azure.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        private readonly string _connectionString;
        private static string _containerRoot;

        public IntegrationTests() : this(null) { }

        public IntegrationTests(string connectionString)
        {
            _connectionString = connectionString;
        }

        [AssemblyInitialize]
        public static void Initialize(TestContext context)
        {
            _containerRoot = $"azuredirectorytests/{DateTime.Now.ToString("yyyyMMddhhmmss")}";

            var azuriteProcess = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "azurite.cmd ",
                    Arguments = "--inMemoryPersistence",
                    // RedirectStandardOutput = true,
                    // RedirectStandardError = true,
                    UseShellExecute = true,
                    CreateNoWindow = false,
                }
            };
            azuriteProcess.Start();
        }

        [TestMethod]
        public void TestReadAndWrite()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange(containerName);
            var (dog, cat, car) = InitializeCatalog(azureDirectory, 1000, expectedDirectory);
            try
            {
                var ireader = DirectoryReader.Open(azureDirectory);
                var searcher = new IndexSearcher(ireader);
                var searchForPhrase = SearchForPhrase(searcher, "dog");
                Assert.AreEqual(dog, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "cat");
                Assert.AreEqual(cat, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "car");
                Assert.AreEqual(car, searchForPhrase);
                Trace.TraceInformation("Tests passsed");
            }
            catch (Exception x)
            {
                Trace.TraceInformation("Tests failed:\n{0}", x);
            }

            AssertFilesAreEqual(azureDirectory, expectedDirectory);
        }

        [TestMethod]
        public void TestReadAndWriteWithSubDirectory()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange($"{containerName}/subdirectory");
            var (dog, cat, car) = InitializeCatalog(azureDirectory, 1000, expectedDirectory);

            try
            {

                var ireader = DirectoryReader.Open(azureDirectory);
                var searcher = new IndexSearcher(ireader);
                var searchForPhrase = SearchForPhrase(searcher, "dog");
                Assert.AreEqual(dog, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "cat");
                Assert.AreEqual(cat, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "car");
                Assert.AreEqual(car, searchForPhrase);
                Trace.TraceInformation("Tests passsed");
            }
            catch (Exception x)
            {
                Trace.TraceInformation("Tests failed:\n{0}", x);
            }

            AssertFilesAreEqual(azureDirectory, expectedDirectory);
        }

        [TestMethod]
        public void TestReadAndWriteWithTwoShardDirectories()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";

            var (azureDirectory1, expectedDirectory1) = Arrange($"{containerName}/shard1");
            var (dog, cat, car) = InitializeCatalog(azureDirectory1, 1000, expectedDirectory1);

            var (azureDirectory2, expectedDirectory2) = Arrange($"{containerName}/shard2");
            var (dog2, cat2, car2) = InitializeCatalog(azureDirectory2, 1000, expectedDirectory2);

            ValidateDirectory(azureDirectory1, dog, cat, car);
            ValidateDirectory(azureDirectory2, dog2, cat2, car2);
            AssertFilesAreEqual(azureDirectory1, expectedDirectory1, "#1 shard1| ");
            AssertFilesAreEqual(azureDirectory2, expectedDirectory2, "#1 shard2| ");

            // delete all azureDirectory1 blobs
            foreach (string file in azureDirectory1.ListAll().Where(x => !x.EndsWith(".lock")))
                azureDirectory1.DeleteFile(file);
            foreach (string file in expectedDirectory1.ListAll())
                expectedDirectory1.DeleteFile(file);

            ValidateDirectory(azureDirectory2, dog2, cat2, car2);

            foreach (string file in azureDirectory2.ListAll().Where(x => !x.EndsWith(".lock")))
                azureDirectory2.DeleteFile(file);
            foreach (string file in expectedDirectory2.ListAll())
                expectedDirectory2.DeleteFile(file);

            AssertFilesAreEqual(azureDirectory1, expectedDirectory1, "#2 shard1| ");
            AssertFilesAreEqual(azureDirectory2, expectedDirectory2, "#2 shard2| ");
        }

        [TestMethod]
        public void TestReadAndWrite_WritingTwoConsecutiveTimes()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange(containerName);

            var (dog, cat, car) = InitializeCatalog(azureDirectory, 500, expectedDirectory);
            var (dog1, cat1, car1) = InitializeCatalog(azureDirectory, 500, expectedDirectory);
            dog += dog1;
            cat += cat1;
            car += car1;

            try
            {
                var ireader = DirectoryReader.Open(azureDirectory);
                var searcher = new IndexSearcher(ireader);
                var searchForPhrase = SearchForPhrase(searcher, "dog");
                Assert.AreEqual(dog, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "cat");
                Assert.AreEqual(cat, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "car");
                Assert.AreEqual(car, searchForPhrase);
                Trace.TraceInformation("Tests passsed");
            }
            catch (Exception x)
            {
                Trace.TraceInformation("Tests failed:\n{0}", x);
            }

            AssertFilesAreEqual(azureDirectory, expectedDirectory);
        }

        [TestMethod]
        public void TestReadAndWriteWithSubDirectory_WritingTwoConsecutiveTimes()
        {
            string containerName =
                $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange($"{containerName}/subdirectory");

            var (dog, cat, car) = InitializeCatalog(azureDirectory, 500, expectedDirectory);
            var (dog1, cat1, car1) = InitializeCatalog(azureDirectory, 500, expectedDirectory);
            dog += dog1;
            cat += cat1;
            car += car1;

            try
            {

                var ireader = DirectoryReader.Open(azureDirectory);
                var searcher = new IndexSearcher(ireader);
                var searchForPhrase = SearchForPhrase(searcher, "dog");
                Assert.AreEqual(dog, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "cat");
                Assert.AreEqual(cat, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "car");
                Assert.AreEqual(car, searchForPhrase);
                Trace.TraceInformation("Tests passsed");
            }
            catch (Exception x)
            {
                Trace.TraceInformation("Tests failed:\n{0}", x);
            }

            AssertFilesAreEqual(azureDirectory, expectedDirectory);
        }

        [TestMethod]
        public void TestReadAndWriteWithTwoShardDirectories_WritingTwoConsecutiveTimes()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";

            var (azureDirectory1, expectedDirectory1) = Arrange($"{containerName}/shard1");
            var (azureDirectory2, expectedDirectory2) = Arrange($"{containerName}/shard2");

            var (dog, cat, car) = InitializeCatalog(azureDirectory1, 500, expectedDirectory1);
            var (dog1, cat1, car1) = InitializeCatalog(azureDirectory1, 500, expectedDirectory1);
            dog += dog1;
            cat += cat1;
            car += car1;

            var (dog2, cat2, car2) = InitializeCatalog(azureDirectory2, 250, expectedDirectory2);
            var (dog3, cat3, car3) = InitializeCatalog(azureDirectory2, 250, expectedDirectory2);
            dog2 += dog3;
            cat2 += cat3;
            car2 += car3;

            ValidateDirectory(azureDirectory1, dog, cat, car);
            ValidateDirectory(azureDirectory2, dog2, cat2, car2);
            AssertFilesAreEqual(azureDirectory1, expectedDirectory1, "#1 shard1| ");
            AssertFilesAreEqual(azureDirectory2, expectedDirectory2, "#1 shard2| ");

            // delete all azureDirectory1 blobs
            foreach (string file in azureDirectory1.ListAll().Where(x => !x.EndsWith(".lock")))
                azureDirectory1.DeleteFile(file);
            foreach (string file in expectedDirectory1.ListAll())
                expectedDirectory1.DeleteFile(file);

            ValidateDirectory(azureDirectory2, dog2, cat2, car2);

            foreach (string file in azureDirectory2.ListAll().Where(x => !x.EndsWith(".lock")))
                azureDirectory2.DeleteFile(file);
            foreach (string file in expectedDirectory2.ListAll())
                expectedDirectory2.DeleteFile(file);

            AssertFilesAreEqual(azureDirectory1, expectedDirectory1, "#2 shard1| ");
            AssertFilesAreEqual(azureDirectory2, expectedDirectory2, "#2 shard2| ");

        }


        [TestMethod]
        public void AzureLock_LockTest()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange(containerName);
            var blobLock = azureDirectory.MakeLock("write.lock");
            var fsLock = expectedDirectory.MakeLock("write.lock");
            Assert.IsNotNull(fsLock);
            Assert.IsNotNull(blobLock);

            Assert.IsFalse(fsLock.IsLocked());
            Assert.IsFalse(blobLock.IsLocked());

            Assert.IsTrue(fsLock.Obtain());
            Assert.IsTrue(blobLock.Obtain());

            Assert.IsTrue(fsLock.IsLocked());
            Assert.IsTrue(blobLock.IsLocked());

            fsLock.Dispose();
            blobLock.Dispose();

            Assert.IsFalse(fsLock.IsLocked());
            Assert.IsFalse(blobLock.IsLocked());

            AssertFilesAreEqual(azureDirectory, expectedDirectory);
        }

        [TestMethod]
        public async Task AzureLock_StressTest()
        {
            // run THREADCOUNT threads each attempting to grab and release the lock ITERATIONS times
            var THREADCOUNT = 50;
            var ITERATIONS = 5;
            var DELAY = 10;
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange(containerName);
            Random rnd = new Random();
            int[] locks = new int[THREADCOUNT];
            int[] releases = new int[THREADCOUNT];
            List<Task> tasks = new List<Task>();
            for (int i = 0; i < THREADCOUNT; i++)
            {
                var blobLock = new AzureLock("write.lock", azureDirectory);
                var instance = i;
                tasks.Add(Task.Run(async () =>
                {
                    for (int y = 0; y < ITERATIONS; y++)
                    {
                        while (!blobLock.Obtain())
                        {
                            await Task.Delay(rnd.Next(DELAY) + DELAY);
                        }
                        // we have lock
                        locks[instance]++;
                        await Task.Delay(rnd.Next(DELAY) + DELAY);
                        blobLock.Dispose();
                        releases[instance]++;
                    }
                }));
            }
            await Task.WhenAll(tasks);
            for (int i = 0; i < THREADCOUNT; i++)
            {
                Assert.AreEqual(ITERATIONS, locks[i]);
                Assert.AreEqual(ITERATIONS, releases[i]);
            }
        }


        [TestMethod]
        public void AzureLockTest()
        {
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            var (azureDirectory, expectedDirectory) = Arrange(containerName);
            var factory = azureDirectory.LockFactory;
            var testLock = factory.MakeLock("write.lock");
            var testLock2 = factory.MakeLock("write.lock");
            var testLock3 = factory.MakeLock("write3.lock");
            Assert.AreSame(testLock, testLock2);
            Assert.AreNotSame(testLock, testLock3);
        }

        [TestMethod]
        public void CanListAllFileNames_InFlatContainer()
        {
            // Arrange
            var expectedFileNames = string.Join("\n", new[]
            {
                "_0.cfe",
                "_0.cfs",
                "_0.si",
                "segments.gen",
                "segments_1"
            });
            string containerName = $"{_containerRoot}/{GetMethodName()}";

            TestListingFilesOfDirectory(containerName, expectedFileNames);
        }

        [TestMethod]
        public void CanListAllFileNames_InLevel1Subdirectory()
        {
            // Arrange
            var expectedFileNames = string.Join("\n", new[]
            {
                "_0.cfe",
                "_0.cfs",
                "_0.si",
                "segments.gen",
                "segments_1"
            });
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            TestListingFilesOfDirectory($"{containerName}/shard1", expectedFileNames);
        }

        [TestMethod]
        public void CanListAllFileNames_InLevel2Subdirectory()
        {
            // Arrange
            var expectedFileNames = string.Join("\n", new[]
            {
                "_0.cfe",
                "_0.cfs",
                "_0.si",
                "segments.gen",
                "segments_1"
            });
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            TestListingFilesOfDirectory($"{containerName}/shard1/level2", expectedFileNames);
        }

        [TestMethod]
        public void CanListAllFileNames_InFlatContainer_After2Writes()
        {
            // Arrange
            var expectedFileNames = string.Join("\n", new[]
            {
               "_0.cfe",
               "_0.cfs",
               "_0.si",
               "_1.cfe",
               "_1.cfs",
               "_1.si",
               "segments.gen",
               "segments_2"
            });
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            TestListingFilesOfDirectory(containerName, expectedFileNames, numberOfSimulatedIndexWrites: 2);
        }

        [TestMethod]
        public void CanListAllFileNames_InLevel1Subdirectory_After2Writes()
        {
            // Arrange
            var expectedFileNames = string.Join("\n", new[]
            {
                "_0.cfe",
                "_0.cfs",
                "_0.si",
                "_1.cfe",
                "_1.cfs",
                "_1.si",
                "segments.gen",
                "segments_2"
            });
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            TestListingFilesOfDirectory($"{containerName}/shard1", expectedFileNames, numberOfSimulatedIndexWrites: 2);
        }

        [TestMethod]
        public void CanListAllFileNames_InLevel2Subdirectory_After2Writes()
        {
            // Arrange
            var expectedFileNames = string.Join("\n", new[]
            {
                "_0.cfe",
                "_0.cfs",
                "_0.si",
                "_1.cfe",
                "_1.cfs",
                "_1.si",
                "segments.gen",
                "segments_2"
            });
            string containerName = $"{_containerRoot}/{GetMethodName()}";
            TestListingFilesOfDirectory($"{containerName}/shard1/level2", expectedFileNames, numberOfSimulatedIndexWrites: 2);
        }

        private string GetMethodName([CallerMemberName] string methodName = null)
        {
            return methodName;
        }

        private void TestListingFilesOfDirectory(string containerName, string expectedFileNames, int numberOfSimulatedIndexWrites = 1)
        {
            var connectionString = _connectionString ?? "UseDevelopmentStorage=true";
            var blobClient = new BlobServiceClient(connectionString);
            var container = blobClient.GetBlobContainerClient(containerName);

            var azureDirectory = new AzureDirectory(connectionString, containerName);

            for (int i = 0; i < numberOfSimulatedIndexWrites; i++)
            {
                InitializeCatalog(azureDirectory, 1000 / numberOfSimulatedIndexWrites);
            }

            // Act
            var actual = azureDirectory.ListAll().Where(x => !x.EndsWith(".lock"));

            // Assert
            var actualFileNames = string.Join("\n", actual);
            Assert.AreEqual(expectedFileNames, actualFileNames);
        }

        private void AssertFilesAreEqual(AzureDirectory azureDirectory, FSDirectory expcetedDirectory, string messagePrefix = null)
        {
            var cacheDirectory = azureDirectory.CacheDirectory;
            var cachedFiles = cacheDirectory.ListAll().OrderBy(x => x).ToList();
            var azureFiles = azureDirectory.ListAll().OrderBy(x => x).Where(x => !x.EndsWith(".lock")).ToList();
            var expectedFiles = expcetedDirectory.ListAll().OrderBy(x => x).ToList();

            var prefix = messagePrefix ?? string.Empty;

            Assert.AreEqual(azureFiles.Count, cachedFiles.Intersect(azureFiles).Count(), $"{prefix}files contained in azure directory must exist in cache");
            Assert.AreEqual(string.Join("\n", azureFiles), string.Join("\n", expectedFiles), $"{prefix}files contained in azure directory and expected directory differ");

            var errors = new List<string>();

            foreach (var f in azureFiles.FilterSiFiles())
            {
                using var actualFile = azureDirectory.OpenInput(f, new IOContext());
                using var expectedFile = expcetedDirectory.OpenInput(f, new IOContext());
                using var cachedFile = cacheDirectory.OpenInput(f, new IOContext());

                byte[] actualData = new byte[actualFile.Length];
                actualFile.ReadBytes(actualData, 0, (int)actualFile.Length);
                byte[] expectedData = new byte[expectedFile.Length];
                expectedFile.ReadBytes(expectedData, 0, (int)expectedFile.Length);
                byte[] cachedData = new byte[cachedFile.Length];
                cachedFile.ReadBytes(cachedData, 0, (int)cachedFile.Length);

                if (expectedFile.Length != actualFile.Length)
                    errors.Add($"{prefix}the FSDirectory and azure files '{f}' differ in length (actual: {actualFile.Length}, expected: {expectedFile.Length})");
                else if (!actualData.SequenceEqual(expectedData))
                    errors.Add($"{prefix}the FSDirectory and azure files '{f}' differ in their content (actual MD5: {Convert.ToBase64String(MD5.HashData(actualData))}, expected MD5: {Convert.ToBase64String(MD5.HashData(expectedData))})");

                if (cachedFile.Length != actualFile.Length)
                    errors.Add($"{prefix}the cached- and azure files '{f}' differ in length (actual: {actualFile.Length}, cachedFile: {cachedFile.Length})");
                else if (!actualData.SequenceEqual(cachedData))
                    errors.Add($"{prefix}the cached- and azure files '{f}' differ in their content (actual MD5: {Convert.ToBase64String(MD5.HashData(actualData))}, cached MD5: {Convert.ToBase64String(MD5.HashData(cachedData))})");
            }

            Assert.IsTrue(errors.Count == 0, string.Join("\n", errors));
        }

        private (AzureDirectory azureDirectory, FSDirectory directory) Arrange(string containerName)
        {
            // create azure dir
            var connectionString = _connectionString ?? "UseDevelopmentStorage=true";
            var blobClient = new BlobServiceClient(connectionString);
            var container = blobClient.GetBlobContainerClient(containerName);
            var azureDirectory = new AzureDirectory(connectionString, containerName);

            // create local dir
            var directory = Path.Combine(Environment.CurrentDirectory, containerName.Replace("/", "\\"));
            var dirInfo = new DirectoryInfo(directory);
            if (dirInfo.Exists)
                dirInfo.Delete(true);
            dirInfo.Create();
            return (azureDirectory, FSDirectory.Open(dirInfo));
        }

        private static void ValidateDirectory(AzureDirectory azureDirectory2, Int32 dog2, Int32 cat2, Int32 car2)
        {
            System.Diagnostics.Debug.WriteLine("--------- DirectoryReader ---------");
            var ireader = DirectoryReader.Open(azureDirectory2);
            for (var i = 0; i < 100; i++)
            {
                var searcher = new IndexSearcher(ireader);
                var searchForPhrase = SearchForPhrase(searcher, "dog");
                Assert.AreEqual(dog2, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "cat");
                Assert.AreEqual(cat2, searchForPhrase);
                searchForPhrase = SearchForPhrase(searcher, "car");
                Assert.AreEqual(car2, searchForPhrase);
            }
            Trace.TraceInformation("Tests passsed");
        }

        private static (int dog, int cat, int car) InitializeCatalog(AzureDirectory azureDirectory, int docs, FSDirectory referenceDirectory = null)
        {
            var indexWriterConfig = new IndexWriterConfig(
                Lucene.Net.Util.LuceneVersion.LUCENE_48,
                new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48));

            var dog = 0;
            var cat = 0;
            var car = 0;

            // if we are passed a reference directory, we also write to it so we can compare the files later
            var referenceIndexWriter = referenceDirectory != null
                ? new IndexWriter(referenceDirectory, new IndexWriterConfig(Lucene.Net.Util.LuceneVersion.LUCENE_48, new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48)))
                : null;

            try
            {
                using (var indexWriter = new IndexWriter(azureDirectory, indexWriterConfig))
                {

                    for (var iDoc = 0; iDoc < docs; iDoc++)
                    {
                        var bodyText = GeneratePhrase(40);
                        var doc = new Document
                        {
                            new TextField("id", DateTime.Now.ToFileTimeUtc() + "-" + iDoc, Field.Store.YES),
                            new TextField("Title", GeneratePhrase(10), Field.Store.YES),
                            new TextField("Body", bodyText, Field.Store.YES)
                        };
                        dog += bodyText.Contains(" dog ") ? 1 : 0;
                        cat += bodyText.Contains(" cat ") ? 1 : 0;
                        car += bodyText.Contains(" car ") ? 1 : 0;
                        indexWriter.AddDocument(doc);
                        referenceIndexWriter?.AddDocument(doc);
                    }

                    Trace.TraceInformation("Total docs is {0}, {1} dog, {2} cat, {3} car", indexWriter.NumDocs, dog,
                        cat, car);
                }
            }
            finally
            {
                referenceIndexWriter?.Dispose();
            }

            return (dog, cat, car);
        }

        private static int SearchForPhrase(IndexSearcher searcher, string phrase)
        {
            var parser = new Lucene.Net.QueryParsers.Classic.QueryParser(Lucene.Net.Util.LuceneVersion.LUCENE_48, "Body", new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48));
            var query = parser.Parse(phrase);
            var topDocs = searcher.Search(query, 100);
            return topDocs.TotalHits;
        }

        private static readonly Random Random = new Random();

        private static readonly string[] SampleTerms = {
            "dog", "cat", "car", "horse", "door", "tree", "chair", "microsoft", "apple", "adobe", "google", "golf",
            "linux", "windows", "firefox", "mouse", "hornet", "monkey", "giraffe", "computer", "monitor",
            "steve", "fred", "lili", "albert", "tom", "shane", "gerald", "chris",
            "love", "hate", "scared", "fast", "slow", "new", "old"
        };

        private static string GeneratePhrase(int maxTerms)
        {
            var phrase = new StringBuilder();
            var nWords = 2 + Random.Next(maxTerms);
            for (var i = 0; i < nWords; i++)
            {
                phrase.AppendFormat(" {0} {1}", SampleTerms[Random.Next(SampleTerms.Length)],
                                    Random.Next(32768).ToString());
            }
            return phrase.ToString();
        }
    }

    internal static class Extensions
    {
        /// <summary>
        /// When comparing file contents, we must omit *.si files since they contain a timestamp and may therefore differ even if they are just fine!
        /// </summary>
        /// <param name="fileNames"></param>
        /// <returns></returns>
        internal static IEnumerable<string> FilterSiFiles(this IEnumerable<string> fileNames)
        {
            return fileNames.Where(f => !f.EndsWith(".si"));
        }
    }
}
