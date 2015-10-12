using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Linq;
using Dapper;
using Hangfire.Annotations;

namespace Hangfire.SQLite
{
    public class SQLiteDistributedLock : IDisposable
    {
        private const int CommandTimeoutAdditionSeconds = 1;

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
            = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());

        private readonly IDbConnection _connection;
        private readonly SQLiteStorage _storage;
        private readonly string _resource;

        private bool _completed;

        public SQLiteDistributedLock([NotNull] SQLiteStorage storage, [NotNull] string resource, TimeSpan timeout)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (String.IsNullOrEmpty(resource)) throw new ArgumentNullException("resource");
            if ((timeout.TotalSeconds + CommandTimeoutAdditionSeconds) > Int32.MaxValue) throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds", Int32.MaxValue - CommandTimeoutAdditionSeconds), "timeout");

            _storage = storage;
            _resource = resource;
            _connection = storage.CreateAndOpenConnection();

            if (!AcquiredLocks.Value.ContainsKey(_resource))
            {
                Acquire(_connection, _resource, timeout);
                AcquiredLocks.Value[_resource] = 1;
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            if (!AcquiredLocks.Value.ContainsKey(_resource)) return;

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] != 0) return;

            try
            {
                Release(_connection, _resource);
                AcquiredLocks.Value.Remove(_resource);
            }
            finally
            {
                _storage.ReleaseConnection(_connection);
            }
        }

        internal void Acquire(IDbConnection connection, string resource, TimeSpan timeout)
        {
            string createLockSql = string.Format(@"insert into [{0}.Lock] (Resource) values (@resource);
                SELECT last_insert_rowid()", _storage.GetSchemaName());

            // Ensuring the timeout for the command is longer than the timeout specified for the stored procedure.
            var commandTimeout = (int)(timeout.TotalSeconds + CommandTimeoutAdditionSeconds);

            var lockId = connection.Query<int>(createLockSql, new { resource }, commandTimeout: commandTimeout).Single();

            if (lockId < 1)
            {
                throw new SQLiteDistributedLockException(
                    String.Format(
                    "Could not place a lock on the resource '{0}': {1}.",
                    resource,
                    String.Format("Server returned '{0}'.", lockId)));
            }
        }

        internal void Release(IDbConnection connection, string resource)
        {
            string deleteLockSql = string.Format(@"delete from [{0}.Lock] where [Resource] = @resource;", _storage.GetSchemaName());

            var delCount = connection.Execute(deleteLockSql, new { resource });

            if (delCount < 1)
            {
                throw new SQLiteDistributedLockException(
                    String.Format(
                    "Could not release a lock on the resource '{0}': Server returned '{1}'.",
                    resource,
                    delCount));
            }
        }
    }
}