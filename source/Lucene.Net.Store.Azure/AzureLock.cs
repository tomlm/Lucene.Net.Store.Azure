//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net;
using Lucene.Net.Store;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Microsoft.Azure.Storage.Blob;
using System.IO;
using Microsoft.Azure.Storage;

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
            var blob = _azureDirectory.BlobContainer.GetBlobReferenceFromServer(_lockFile);
            try
            {
                Debug.WriteLine($"{_azureDirectory.Name} IsLocked() : {_leaseid}");
                if (String.IsNullOrEmpty(_leaseid))
                {
                    var tempLease = blob.AcquireLease(TimeSpan.FromSeconds(60), _leaseid);
                    if (String.IsNullOrEmpty(tempLease))
                    {
                        Debug.Print("IsLocked() : TRUE");
                        return true;
                    }
                    blob.ReleaseLease(new AccessCondition() { LeaseId = tempLease });
                }
                Debug.Print($"{_azureDirectory.Name} IsLocked() : {_leaseid}");
                return String.IsNullOrEmpty(_leaseid);
            }
            catch (StorageException webErr)
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
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                Debug.WriteLine($"{_azureDirectory.Name} AzureLock:Obtain({_lockFile}) : {_leaseid}");
                if (String.IsNullOrEmpty(_leaseid))
                {
                    _leaseid = blob.AcquireLease(TimeSpan.FromSeconds(60), _leaseid);
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
            catch (StorageException webErr)
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
                var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
                blob.RenewLease(new AccessCondition { LeaseId = _leaseid });
            }
        }

        #endregion

        public void BreakLock()
        {
            Debug.Print("AzureLock:BreakLock({0}) {1}", _lockFile, _leaseid);
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                blob.BreakLease();
            }
            catch (Exception err)
            {
            }
            _leaseid = null;
        }

        public override System.String ToString()
        {
            return $"{_azureDirectory.Name} AzureLock@{_lockFile}.{_leaseid}";
        }

        private bool _handleWebException(ICloudBlob blob, StorageException err)
        {
            if (err.RequestInformation.HttpStatusCode == 404)
            {
                _azureDirectory.CreateContainer();
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_lockFile);
                    blob.UploadFromStream(stream);
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
                var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
                blob.ReleaseLease(new AccessCondition { LeaseId = _leaseid });
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

