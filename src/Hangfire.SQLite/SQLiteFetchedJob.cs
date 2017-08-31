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
using Hangfire.Storage;
using System;

namespace Hangfire.SQLite
{
    internal class SQLiteFetchedJob : IFetchedJob
    {
        private readonly SQLiteStorage _storage;       

        public SQLiteFetchedJob(
            [NotNull] SQLiteStorage storage,
            int id,           
            string jobId,
            string queue)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));            
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            _storage = storage;

            Id = id;
            JobId = jobId;
            Queue = queue;
        }

        public int Id { get; private set; }
        public string JobId { get; private set; }
        public string Queue { get; private set; }

        public void RemoveFromQueue()
        {
            _storage.UseConnection(connection =>
            {
                connection.Execute($@"delete from [{_storage.SchemaName}.JobQueue] where Id = @id",
                    new { id = Id });
            }, true);
        }

        public void Requeue()
        {
            _storage.UseConnection(connection =>
            {
                connection.Execute($@"update [{_storage.SchemaName}.JobQueue] set FetchedAt = null where Id = @id",
                    new { id = Id });
            }, true);
        }

        public void Dispose()
        {
            
        }
    }
}
