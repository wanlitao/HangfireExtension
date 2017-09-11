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

using Dapper;
using Hangfire.Annotations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

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
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            string sqlQuery = $@"select distinct(Queue) from [{_storage.SchemaName}.JobQueue]";

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
            string sqlQuery = 
            $@"select JobId from [{_storage.SchemaName}.JobQueue]
                where Queue = @queue and FetchedAt is null
               order by Id
               limit @limit offset @offset";

            return UseConnection(connection =>
            {
                return connection.Query<JobIdDto>(
                    sqlQuery,
                    new { queue = queue, limit = perPage, offset = @from })
                    .ToList()
                    .Select(x => x.JobId)
                    .ToList();
            });
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            string fetchedJobsSql =
            $@"select JobId from [{_storage.SchemaName}.JobQueue]
                where Queue = @queue and FetchedAt is not null
               order by Id
               limit @limit offset @offset";

            return UseConnection(connection =>
            {
                return connection.Query<JobIdDto>(
                    fetchedJobsSql,
                    new { queue = queue, limit = perPage, offset = @from })
                    .ToList()
                    .Select(x => x.JobId)
                    .ToList();
            });            
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var sqlQuery = $@"
select sum(Enqueued) as EnqueuedCount, sum(Fetched) as FetchedCount 
from (
    select 
        case when FetchedAt is null then 1 else 0 end as Enqueued,
        case when FetchedAt is not null then 1 else 0 end as Fetched
    from [{_storage.SchemaName}.JobQueue]
    where Queue = @queue
) q";

            return UseConnection(connection =>
            {
                var result = connection.Query(sqlQuery, new { queue = queue }).Single();

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result.EnqueuedCount,
                    FetchedCount = result.FetchedCount
                };
            });
        }

        private T UseConnection<T>(Func<DbConnection, T> func, bool isWriteLock = false)
        {
            return _storage.UseConnection(func, isWriteLock);
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private class JobIdDto
        {
            public int JobId { get; set; }
        }
    }
}