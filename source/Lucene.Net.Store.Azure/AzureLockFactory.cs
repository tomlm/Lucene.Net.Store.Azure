using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Lucene.Net.Store.Azure
{
    public class AzureLockFactory : LockFactory
    {
        private readonly AzureDirectory _azureDirectory;
        private readonly Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();

        public AzureLockFactory(AzureDirectory directory)
        {
            _azureDirectory = directory;
        }

        public override Lock MakeLock(string lockName)
        {
            lock (_locks)
            {
                if (!_locks.TryGetValue(lockName, out var azureLock))
                {
                    azureLock = new AzureLock(lockName, _azureDirectory);
                    _locks[lockName] = azureLock;
                }
                return azureLock;
            }
        }

        public override void ClearLock(string lockName)
        {
            lock (_locks)
            {
                if (!_locks.TryGetValue(lockName, out var azureLock))
                {
                    azureLock = new AzureLock(lockName, _azureDirectory);
                    _locks[lockName] = azureLock;
                }
                azureLock.BreakLock();
            }
        }
    }
}
