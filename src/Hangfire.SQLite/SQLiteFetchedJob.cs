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
using Dapper;
using Hangfire.Storage;
using Hangfire.Annotations;

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
            if (storage == null) throw new ArgumentNullException("storage");            
            if (jobId == null) throw new ArgumentNullException("jobId");
            if (queue == null) throw new ArgumentNullException("queue");

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
                connection.Execute(string.Format(@"
                    delete from [{0}.JobQueue] where Id = @id", _storage.GetSchemaName()),
                    new { id = Id });
            }, true);
        }

        public void Requeue()
        {
            _storage.UseConnection(connection =>
            {
                connection.Execute(string.Format(@"
                    update [{0}.JobQueue] set FetchedAt = null where Id = @id", _storage.GetSchemaName()),
                    new { id = Id });
            }, true);
        }

        public void Dispose()
        {
            
        }
    }
}
