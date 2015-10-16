using System;
using System.Linq;
using Dapper;
using Hangfire.Annotations;
using System.Threading;

namespace Hangfire.SQLite
{
    public class SQLiteDistributedLock : IDisposable
    {
        private readonly SQLiteStorage _storage;
        private readonly string _resource;
        private static readonly TimeSpan WaitBetweenAttempts = TimeSpan.FromSeconds(1);        

        private bool _completed;

        public SQLiteDistributedLock([NotNull] SQLiteStorage storage, [NotNull] string resource, TimeSpan timeout)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (String.IsNullOrEmpty(resource)) throw new ArgumentNullException("resource");
            if (timeout.TotalSeconds > Int32.MaxValue) throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds", Int32.MaxValue), "timeout");

            _storage = storage;
            _resource = resource;            
            
            Acquire(_resource, timeout);
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            Release(_resource);            
        }

        internal void Acquire(string resource, TimeSpan timeout)
        {
            string createLockSql = string.Format(@"insert into [{0}.Lock] (Resource) values (@resource);
                SELECT last_insert_rowid()", _storage.GetSchemaName());

            long lockId = 0;
            var lockRetryEndTime = DateTime.UtcNow + timeout;

            _storage.UseConnection(connection =>
            {
                while (true)
                {
                    try
                    {
                        lockId = connection.Query<long>(createLockSql, new { resource }).Single();
                    }
                    catch (Exception)
                    {
                        lockId = 0;
                    }

                    if (lockId > 0)
                        break;

                    Thread.Sleep(WaitBetweenAttempts);

                    if (DateTime.UtcNow > lockRetryEndTime)
                    {
                        throw new SQLiteDistributedLockException(
                            String.Format(
                            "Could not place a lock on the resource '{0}': {1}.",
                            resource,
                            String.Format("Server returned '{0}'.", lockId)));
                    }
                }
            }, true);
        }

        internal void Release(string resource)
        {
            string deleteLockSql = string.Format(@"delete from [{0}.Lock] where [Resource] = @resource;", _storage.GetSchemaName());

            _storage.UseConnection(connection =>
            {
                var delCount = connection.Execute(deleteLockSql, new { resource });

                if (delCount < 1)
                {
                    throw new SQLiteDistributedLockException(
                        String.Format(
                        "Could not release a lock on the resource '{0}': Server returned '{1}'.",
                        resource,
                        delCount));
                }
            }, true);
        }
    }
}