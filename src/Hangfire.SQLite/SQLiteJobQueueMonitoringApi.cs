// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.Annotations;
#if NETSTANDARD2_0
using SQLiteConnection = Microsoft.Data.Sqlite.SqliteConnection;
#else
using System.Data.SQLite;
#endif

namespace Hangfire.SQLite
{
    internal class SQLiteJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);

        private readonly SQLiteStorage _storage;
        private readonly object _cacheLock = new object();

        private List<string> _queuesCache = new List<string>();
        private DateTime _cacheUpdated;

        public SQLiteJobQueueMonitoringApi([NotNull] SQLiteStorage storage)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            string sqlQuery = string.Format(@"select distinct(Queue) from [{0}.JobQueue]", _storage.GetSchemaName());

            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Add(QueuesCacheTimeout) < DateTime.UtcNow)
                {
                    var result = UseConnection(connection =>
                    {
                        return connection.Query(sqlQuery).Select(x => (string)x.Queue).ToList();
                    });

                    _queuesCache = result;
                    _cacheUpdated = DateTime.UtcNow;
                }

                return _queuesCache.ToList();
            }
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            string sqlQuery = string.Format(@"
               select Id from [{0}.JobQueue]
                where Queue = @queue
               order by Id
               limit @limit offset @offset", _storage.GetSchemaName());

            return UseConnection(connection =>
            {
                return connection.Query<JobIdDto>(
                    sqlQuery,
                    new { queue = queue, limit = perPage, offset = @from })
                    .ToList()
                    .Select(x => x.Id)
                    .ToList();
            });
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return Enumerable.Empty<int>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            string sqlQuery = string.Format(@"
select count(Id) from [{0}.JobQueue] where [Queue] = @queue", _storage.GetSchemaName());

            return UseConnection(connection =>
            {
                var result = connection.Query<int>(sqlQuery, new { queue = queue }).Single();

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result,
                };
            });
        }

        private T UseConnection<T>(Func<SQLiteConnection, T> func, bool isWriteLock = false)
        {
            return _storage.UseConnection(func, isWriteLock);
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private class JobIdDto
        {
            public int Id { get; set; }
        }
    }
}