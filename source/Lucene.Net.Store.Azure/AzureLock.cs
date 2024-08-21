//    License: Microsoft Public License (Ms-PL) 
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements lock semantics on AzureDirectory via a blob lease
    /// </summary>
    public class AzureLock : Lock
    {
        private string _lockFile, _lockName;
        private AzureDirectory _azureDirectory;
        private string _leaseid;

        public AzureLock(string lockFile, AzureDirectory directory)
        {
            _lockName = lockFile;
            _lockFile = directory.GetBlobName(lockFile);
            _azureDirectory = directory;
        }

        #region Lock methods
        override public bool IsLocked()
        {
            var blob = _azureDirectory.BlobContainer.GetBlobClient(_lockFile);
            try
            {
                Debug.WriteLine($"{_azureDirectory.Name} IsLocked() : {_leaseid}");
                if (String.IsNullOrEmpty(_leaseid))
                {
                    var lease = blob.GetBlobLeaseClient();
                    var tempLease = lease.Acquire(TimeSpan.FromSeconds(60));
                    if (String.IsNullOrEmpty(tempLease.Value.LeaseId))
                    {
                        Debug.Print("IsLocked() : TRUE");
                        // we were not able to get a temp lease
                        return true;
                    }
                    lease.Release();
                    return false;
                }
                Debug.Print($"{_azureDirectory.Name} IsLocked() : {_leaseid}");
                return !String.IsNullOrEmpty(_leaseid);
            }
            catch (RequestFailedException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return IsLocked();
            }
            /*catch (StorageClientException err)
            {
                if (_handleStorageClientException(blob, err))
                    return IsLocked();
            }*/
            _leaseid = null;
            return false;
        }

        public override bool Obtain()
        {
            var blob = _azureDirectory.BlobContainer.GetBlobClient(_lockFile);
            try
            {
                if (String.IsNullOrEmpty(_leaseid))
                {
                    var lease = blob.GetBlobLeaseClient().Acquire(TimeSpan.FromSeconds(60));
                    _leaseid = lease.Value.LeaseId;
                    Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Obtain({_lockFile}): AcquiredLease : {_leaseid}");

                    // keep the lease alive by renewing every 30 seconds
                    long interval = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
                    _renewTimer = new Timer((obj) =>
                    {
                        try
                        {
                            AzureLock al = (AzureLock)obj;
                            al.Renew();
                        }
                        catch (Exception err) { Debug.Print(err.ToString()); }
                    }, this, interval, interval);
                }
                return !String.IsNullOrEmpty(_leaseid);
            }
            catch (RequestFailedException webErr)
            {
                switch ((HttpStatusCode)webErr.Status)
                {
                    case HttpStatusCode.NotFound:
                        if (_handleWebException(blob, webErr))
                            return Obtain();
                        break;
                    case HttpStatusCode.Conflict:
                        // lease not available
                        break;
                }
            }
            /*catch (StorageClientException err)
            {
                if (_handleStorageClientException(blob, err))
                    return Obtain();
            }*/
            Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Obtain({_lockFile}) : LOCKED");
            return false;
        }

        private Timer _renewTimer;

        public void Renew()
        {
            if (!String.IsNullOrEmpty(_leaseid))
            {
                Debug.Print("AzureLock:Renew({0} : {1}", _lockFile, _leaseid);
                var blob = _azureDirectory.BlobContainer.GetBlobClient(_lockFile);
                blob.GetBlobLeaseClient(_leaseid).Renew();
            }
        }

        #endregion

        public void BreakLock()
        {
            Debug.Print("AzureLock:BreakLock({0}) {1}", _lockFile, _leaseid);
            var blob = _azureDirectory.BlobContainer.GetBlobClient(_lockFile);
            try
            {
                if (!String.IsNullOrEmpty(_leaseid))
                    blob.GetBlobLeaseClient(_leaseid).Release();
                // blob.GetBlobLeaseClient(_leaseid).Break();
            }
            catch (RequestFailedException)
            {
            }
            blob.Delete();
            _leaseid = null;
        }

        public override System.String ToString()
        {
            return $"{_azureDirectory.Name} AzureLock@{_lockFile}.{_leaseid}";
        }

        private bool _handleWebException(BlobClient blob, RequestFailedException err)
        {
            if (err.Status == 404)
            {
                _azureDirectory.CreateContainer();
                try
                {

                    using (var stream = new MemoryStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        writer.Write(_lockFile);
                        blob.Upload(stream);
                    }
                }
                catch (RequestFailedException err2)
                {
                    if (err2.Status == 409)
                        return true;
                    throw;
                }
                return true;
            }
            return false;
        }

        protected override void Dispose(Boolean disposing)
        {
            Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Release({_lockFile}) {_leaseid}");
            if (!String.IsNullOrEmpty(_leaseid))
            {
                if (_renewTimer != null)
                {
                    _renewTimer.Dispose();
                    _renewTimer = null;
                }
                Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Dispose({_lockFile}): ReleasedLease: {_leaseid}");
                var blob = _azureDirectory.BlobContainer.GetBlobClient(_lockFile);
                var response = blob.GetBlobLeaseClient(_leaseid).Release();
                _leaseid = null;
            }
        }
    }

}

