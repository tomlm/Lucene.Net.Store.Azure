//    License: Microsoft Public License (Ms-PL) 
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements lock semantics on AzureDirectory via a blob lease
    /// </summary>
    public class AzureLock : Lock
    {
        private string _lockFile;
        private AzureDirectory _azureDirectory;
        private string _leaseid;

        public AzureLock(string lockFile, AzureDirectory directory)
        {
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
                        return true;
                    }
                    lease.Release();
                }
                Debug.Print($"{_azureDirectory.Name} IsLocked() : {_leaseid}");
                return String.IsNullOrEmpty(_leaseid);
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
                Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Obtain({_lockFile}) : {_leaseid}");
                if (String.IsNullOrEmpty(_leaseid))
                {
                    var lease = blob.GetBlobLeaseClient().Acquire(TimeSpan.FromSeconds(60));
                    _leaseid = lease.Value.LeaseId;
                    Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Obtain({_lockFile}): AcquireLease : {_leaseid}");

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
                if (_handleWebException(blob, webErr))
                    return Obtain();
            }
            /*catch (StorageClientException err)
            {
                if (_handleStorageClientException(blob, err))
                    return Obtain();
            }*/
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
                blob.GetBlobLeaseClient(_leaseid).Break();
            }
            catch (RequestFailedException err)
            {
            }
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
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_lockFile);
                    blob.Upload(stream);
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
                var blob = _azureDirectory.BlobContainer.GetBlobClient(_lockFile);
                blob.GetBlobLeaseClient(_leaseid).Release();
                if (_renewTimer != null)
                {
                    _renewTimer.Dispose();
                    _renewTimer = null;
                }
                _leaseid = null;
            }
        }
    }

}

