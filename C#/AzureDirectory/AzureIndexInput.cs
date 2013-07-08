﻿//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Lucene.Net;
using Lucene.Net.Store;
using System.Diagnostics;
using System.IO.Compression;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput
    {
        private AzureDirectory _azureDirectory;
        private CloudBlobContainer _blobContainer;
        private ICloudBlob _blob;
        private string _name;

        private IndexInput _indexInput;
        private Mutex _fileMutex;

        public Lucene.Net.Store.Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }
        private static long ticks1970 = new DateTime(1970, 1, 1, 0, 0, 0).Ticks / TimeSpan.TicksPerMillisecond;

        public AzureIndexInput(AzureDirectory azuredirectory, ICloudBlob blob)
        {
            _name = blob.Uri.Segments[blob.Uri.Segments.Length - 1];

#if FULLDEBUG
            Debug.WriteLine(String.Format("opening {0} ", _name));
#endif
            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azuredirectory;
                _blobContainer = azuredirectory.BlobContainer;
                _blob = blob;

                string fileName = _name;

                bool fFileNeeded = false;
                if (!CacheDirectory.FileExists(fileName))
                {
                    fFileNeeded = true;
                }
                else
                {
                    long cachedLength = CacheDirectory.FileLength(fileName);
                    long blobLength = blob.Properties.Length;
                    long.TryParse(blob.Metadata["CachedLength"], out blobLength);

                    long longLastModified = 0;
                    DateTime blobLastModifiedUTC = blob.Properties.LastModified.Value.UtcDateTime;
                    if (long.TryParse(blob.Metadata["CachedLastModified"], out longLastModified))
                    {
                        // normalize RAMDirectory and FSDirectory times
                        if (longLastModified > ticks1970)
                            longLastModified -= ticks1970;
                        blobLastModifiedUTC = new DateTime(longLastModified).ToUniversalTime();
                    }

                    if (cachedLength != blobLength)
                        fFileNeeded = true;
                    else
                    {
                        // there seems to be an error of 1 tick which happens every once in a while 
                        // for now we will say that if they are within 1 tick of each other and same length 

                        var elapsed = CacheDirectory.FileModified(fileName);

                        // normalize RAMDirectory and FSDirectory times
                        if (elapsed > ticks1970)
                            elapsed -= ticks1970;

                        DateTime cachedLastModifiedUTC = new DateTime(elapsed, DateTimeKind.Local).ToUniversalTime();
                        if (cachedLastModifiedUTC != blobLastModifiedUTC)
                        {
                            TimeSpan timeSpan = blobLastModifiedUTC.Subtract(cachedLastModifiedUTC);
                            if (timeSpan.TotalSeconds > 1)
                                fFileNeeded = true;
                            else
                            {
#if FULLDEBUG
                                Debug.WriteLine(timeSpan.TotalSeconds);
#endif
                                // file not needed
                            }
                        }
                    }
                }

                // if the file does not exist
                // or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
                if (fFileNeeded)
                {
#if COMPRESSBLOBS
                    if (_azureDirectory.ShouldCompressFile(_name))
                    {
                        // then we will get it fresh into local deflatedName 
                        // StreamOutput deflatedStream = new StreamOutput(CacheDirectory.CreateOutput(deflatedName));
                        MemoryStream deflatedStream = new MemoryStream();

                        // get the deflated blob
                        _blob.DownloadToStream(deflatedStream);

                        Debug.WriteLine(string.Format("GET {0} RETREIVED {1} bytes", _name, deflatedStream.Length));

                        // seek back to begininng
                        deflatedStream.Seek(0, SeekOrigin.Begin);

                        // open output file for uncompressed contents
                        StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName);

                        // create decompressor
                        DeflateStream decompressor = new DeflateStream(deflatedStream, CompressionMode.Decompress);

                        byte[] bytes = new byte[65535];
                        int nRead = 0;
                        do
                        {
                            nRead = decompressor.Read(bytes, 0, 65535);
                            if (nRead > 0)
                                fileStream.Write(bytes, 0, nRead);
                        } while (nRead == 65535);
                        decompressor.Close(); // this should close the deflatedFileStream too

                        fileStream.Close();

                    }
                    else
#endif
                    {
                        StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName);

                        // get the blob
                        _blob.DownloadToStream(fileStream);

                        fileStream.Flush();
                        Debug.WriteLine(string.Format("GET {0} RETREIVED {1} bytes", _name, fileStream.Length));

                        fileStream.Close();
                    }

                    // and open it as an input 
                    _indexInput = CacheDirectory.OpenInput(fileName);
                }
                else
                {
#if FULLDEBUG
                    Debug.WriteLine(String.Format("Using cached file for {0}", _name));
#endif

                    // open the file in read only mode
                    _indexInput = CacheDirectory.OpenInput(fileName);
                }
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public AzureIndexInput(AzureIndexInput cloneInput)
        {
            _fileMutex = BlobMutexManager.GrabMutex(cloneInput._name);
            _fileMutex.WaitOne();

            try
            {
#if FULLDEBUG
                Debug.WriteLine(String.Format("Creating clone for {0}", cloneInput._name));
#endif
                _azureDirectory = cloneInput._azureDirectory;
                _blobContainer = cloneInput._blobContainer;
                _blob = cloneInput._blob;
                _indexInput = cloneInput._indexInput.Clone() as IndexInput;
            }
            catch (Exception)
            {
                // sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet
                // but this covers our tail until I do
                Debug.WriteLine(String.Format("Dagnabbit, falling back to memory clone for {0}", cloneInput._name));
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override byte ReadByte()
        {
            return _indexInput.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            _indexInput.ReadBytes(b, offset, len);
        }

        public override long FilePointer
        {
            get
            {
                return _indexInput.FilePointer;
            }
        }

        public override void Seek(long pos)
        {
            _indexInput.Seek(pos);
        }

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

        public override long Length()
        {
            return _indexInput.Length();
        }

        public override System.Object Clone()
        {
            IndexInput clone = null;
            try
            {
                _fileMutex.WaitOne();
                AzureIndexInput input = new AzureIndexInput(this);
                clone = (IndexInput)input;
            }
            catch (System.Exception err)
            {
                Debug.WriteLine(err.ToString());
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
            Debug.Assert(clone != null);
            return clone;
        }

    }
}

