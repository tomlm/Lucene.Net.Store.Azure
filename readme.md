# Lucene.Net.Store.Azure (Full Text Indexing for Azure)

## Project description
This project allows you to create Lucene Indexes via a Lucene Directory object which uses Azure BlobStorage for persistent storage. 


## About
This project allows you to create Lucene Indexes via a Lucene Directory object which uses Azure BlobStorage for persistent storage. 

## Background
### Lucene.NET
**Lucene** is a mature Java based open source full text indexing and search engine and property store.

**Lucene.NET** is a mature port of that to C#

**Lucene/Lucene.Net** provides:
* Super simple API for storing documents with arbitrary properties 
* Complete control over what is indexed and what is stored for retrieval 
* Robust control over where and how things are indexed, how much memory is used, etc. 
* Superfast and super rich query capabilities 
* Sorted results 
* Rich constraint semantics AND/OR/NOT etc. 
* Rich text semantics (phrase match, wildcard match, near, fuzzy match etc) 
* Text query syntax (example: Title:(dog AND cat) OR Body:Lucen* ) 
* Programmatic expressions 
* Ranked results with custom ranking algorithms 
 
### AzureDirectory class
**Lucene.Net.Store.Azure** implements a **Directory** storage provider called **AzureDirectory** which smartly uses local file storage to cache files as they are created and automatically pushes them to blob storage as appropriate. Likewise, it smartly caches blob files back to the a client when they change. This provides with a nice blend of just in time syncing of data local to indexers or searchers across multiple machines.

With the flexibility that Lucene provides over data in memory versus storage and the just in time blob transfer that AzureDirectory provides you have great control over the composibility of where data is indexed and how it is consumed.

To be more concrete: you can have 1..N worker roles adding documents to an index, and 1..N searcher webroles searching over the catalog in near real time.

## Usage

To use you need to create a blobstorage account on http://azure.com.

Create an App.Config or Web.Config and configure your accountinto:
```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
  <appSettings>
  <!-- azure SETTINGS -->
  <add key="BlobStorageEndpoint" value="http://YOURACCOUNT.blob.core.windows.net"/>
  <add key="AccountName" value="YOURACCOUNTNAME"/>
  <add key="AccountSharedKey" value="YOURACCOUNTKEY"/>
  </appSettings>
</configuration>
```

To add documents to a catalog is as simple as
```c#
AzureDirectory azureDirectory = new AzureDirectory("TestCatalog");
IndexWriter indexWriter = new IndexWriter(azureDirectory, new StandardAnalyzer(), true);
Document doc = new Document();
doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
doc.Add(new Field("Title", “this is my title”, Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
doc.Add(new Field("Body", “This is my body”, Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
indexWriter.AddDocument(doc);
indexWriter.Close();
```

And searching is as easy as:
```c#
  IndexSearcher searcher = new IndexSearcher(azureDirectory);                
  Lucene.Net.QueryParsers.QueryParser parser = QueryParser("Title", new StandardAnalyzer());
  Lucene.Net.Search.Query query = parser.Parse("Title:(Dog AND Cat)");

  Hits hits = searcher.Search(query);
  for (int i = 0; i < hits.Length(); i++)
  {
      Document doc = hits.Doc(i);
      Console.WriteLine(doc.GetField("Title").StringValue());
  }
```

### Caching and Compression

AzureDirectory compresses blobs before sent to the blob storage. Blobs are automatically cached local to reduce roundtrips for blobs which haven't changed. 

By default AzureDirectory stores this local cache in a temporary folder. You can easily control where the local cache is stored by passing in a Directory object for whatever type and location of storage you want.

This example stores the cache in a ram directory:
```c#
AzureDirectory azureDirectory = new AzureDirectory("MyIndex", new RAMDirectory());
```

And this example stores in the file system in C:\myindex
```c#
AzureDirectory azureDirectory = new AzureDirectory("MyIndex", new FSDirectory(@"c:\myindex"));
```


## Notes on settings

Just like a normal lucene index, calling optimize too often causes a lot of churn and not calling it enough causes too many segment files to be created, so call it "just enough" times. That will totally depend on your application and the nature of your pattern of adding and updating items to determine (which is why lucene provides so many knobs to configure it's behavior).

The default compound file support that Lucene uses is to reduce the number of files that are generated...this means it deletes and merges files regularly which causes churn on the blob storage. Calling indexWriter.SetCompoundFiles(false) will give better performance, because more files means smaller blobs in blob storage and smaller network transactions because you only have to fetch new segments instead of merged segments.

We run it with a RAMDirectory for local cache and SetCompoundFiles(false); 

The version of Lucene.NET checked in as a binary is Version 2.3.1, but you can use any version of Lucene.NET you want by simply enlisting from the above open source site.

# FAQ
 
## How does this relate to Azure Tables?

Lucene doesn’t have any concept of tables. Lucene builds its own property store on top of the Directory() storage abstraction which essentially is both query and storage so it replicates the functionality of tables. You have to question the benefit of having tables in this case.

With LinqToLucene you can have Linq and strongly typed objects just like table storage. Ultimately, Table storage is just an abstraction on top of blob storage, and so is Lucene (a table abstraction on top of blob storage).

Stated another way, just about anything you can build on table storage you can build on lucene storage.

If it is important that you have table storage as well as an Lucene index then any time you create a table entity you simply add that Entity to lucene as a document (either by a simply hand mapping or via reflection Linq To Lucene Annotations) as well. Queries can then be against lucene, and properties retrieved from table storage or from Lucene.

But if you think about it you are duplicating your data then and not really getting much benefit. 

There is 1 benefit to the table storage, and that is as an archive of the state of your data. If for some reason you need to rebuild your index you can simply reconstitute it from the table storage, but that’s probably the only time you would use the table storage then.

## How does this perform?
Lucene is capable of complex searches over millions of records in sub second times depending on how it is configured. 
see http://lucene.apache.org/java/2_3_2/benchmarks.html for lots of details about Lucene in general.

But really this is a totally open ended question. It depends on:
* the amount of data 
* the frequency of updates 
* the kind of schema
* The kind of queries
etc. 
Like any flexible system you can configure it to be supremely performant or supremely unperformant. 

The key to getting good performance is for you to understand how Lucene works. 

Lucene performs efficient incremental indexing by always appending data into files called segments. Periodically it will merge smaller segments into larger segments (a merge). The important thing to know is that it will NEVER modify an old segment, but instead will create new segments and then delete old segments when they are no longer in use. 

Lucene is built on top of an abstract storage class called a "Directory" object, and the Azure Library creates an implementation of that class called "AzureDirectory". The directory contract basically provides:
* the ability to enumerate segments 
* the ability to delete segments 
* providing a stream for Writing a file 
* providing a stream for Reading a file 
etc. 

Existing Directory objects in Lucene are:
* *RAMDirectory* -- a in memory directory implementation 
* *FSDirectory* -- a disk backed directory implementation 

The AzureDirectory class implements the Directory contract as a wrapper around another Directory class which it uses as a local cache. 

* When Lucene asks to enumerate segments, AzureDirectory enumerates the segments in blob storage.
* When Lucene asks to delete a segment, the AzureDirectory deletes the local cache segment and the blob in blob storage.
* When Lucene asks to for a read stream for a segment (remember segments never change after being closed) AzureDirectory looks to see if it is in the local cache Directory, and if it is, simply returns the local cache stream for that segment. Otherwise it fetches the segment from blobstorage, stores it in the local cache Directory and then returns the local cache steram for that segment.
* When Lucene needs to do a write, it asks the directory for a lock.  This lock is at the catalog level and for AzureDirectory it is implemented using Blob Storage locks.
* When Lucene asks for a write stream for a segment it returns a wrapper around the stream in the local Directory cache, and on close it pushes the data up to a blob in blob storage. 

The net result is that:
* all read operations will be performed against the local cache Directory object (which if it is a RAMDirectory is near instaneous). 
* Any time a segment is missing in the local cache you will incure the cost of downloading the segment once. 
* All Write operations are performed against the local cache Directory object until the segment is closed, at which point you incur the cost of uploading the segment, but with a lock to prevent other writes to the catalog.

> NOTE: Lucene only always one writer at a time per catalog. If you want multiple machines to be writing at the same time you can increase your write throughput by having multiple catalogs, each with a writer.  Then your searcher instances can create a MultiSearcher() which searches over all of the catalogs.  This allows you to scale your writers and searchers independently. 

The key piece to understand is that the amount of transactions you have to perform to blob storage depends on the Lucene settings which control how many segments you have before they are merged into a bigger segment (mergeFactor). Calling Optimize() is a really bad idea because it causes ALL SEGMENTS to be merged into ONE SEGMENT...essentially causing the entire index to have to be recreated, uploaded to blob storage and downloaded to all consumers.

The other big factor is how often you create your searcher objects. When you create a Lucene Searcher object it essentially binds to the view of the index at that point in time. Regardless of how many updates are made to the index by other processes, the searcher object will have a static view of the index in it's local cache Directory object. If you want to update the view of the searcher, you simply discard the old one and create a new one and again it will be up to date for the current state of the index.

If you control those factors, you can have a super scalable fast system which can handle millions of records and thousands of queries per second no problem.

## What is the best way to build an Azure application around this?
Of course that depends on your data flow, etc. but in general here is an example architecture that works well:

As noted each catalog can only be updated by one process at a time, so it makes sense to push all Add/Update/Delete operations through an indexing role. The obvious way to do that is to have an Azure queue which feeds a stream of objects to be indexed a worker role which maintains updating a catalog.  If you want to scale write throughput use one queue per catalog and one writer per queue. 

On the search side, you can have a search WebRole which simply creates an AzureDirectory with a RAMDirectory pointed to the blob storage the indexing role is maintaining. As appropriate (say once a minute) the searcher webrole would create a new IndexSearcher object around the index, and any changes will automatically be synced into the cache directory on the searcher webRole.

To scale your search engine you can simply increase the instance count of the searcher webrole to handle the load.
 
# Version History
## Version 1.0.5
* Replaced existing of blob lock file with blob leases to prevent orphaned lock files from happening

## Version 1.0.4
Replaced mutx with BlobMutexManager to solve local mutex permissions
Thanks to Andy Hitchman for the bug fixes

## Version 1.0.3
* Added a call to persist the CachedLength and CachedLastModified metadata properties to the blob (not included in the content upload). 
* AzureDirectory.FileLength was using the actual blob length rather than the CachedLength property. The latest version of lucene checks the length after closing an index to verify that its correct and was throwing an exception for compressed blobs. 
* Non-compressed blobs were not being uploaded 
* Updated the AzureDirectory constructor to use a CloudStorageAccount rather than the StorageCredentialsAccountAndKey so its possible to use the Development store for testing 
works with Lucene.NET 2.9.2 
** thanks to Joel Fillmore for the bug fixes**

## Version 1.0.2
* updated to use Azure SDK 1.2 

## Version 1.0.1
* rewritten to use V1.1 of Azure SDK and the azure storage client 
* released to MSDN Code Gallery under the MS-PL license. 

## Version 1.0
* Initial release- written for V1.0 CTP of Azure using the sample storage lib 
* Released under restrictive MSR license on http://research.microsoft.com 
 
