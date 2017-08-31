// This file is part of Hangfire.
// Copyright ?2013-2014 Sergey Odinokov.
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
using Hangfire.Common;
using Hangfire.SQLite.Entities;
using Hangfire.States;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Hangfire.SQLite
{
    internal class SQLiteWriteOnlyTransaction : JobStorageTransaction
    {
        //private readonly Queue<Action<System.Data.SQLite.SQLiteConnection>> _commandQueue = new Queue<Action<System.Data.SQLite.SQLiteConnection>>();
        private readonly Queue<Action<DbConnection, DbTransaction>> _commandQueue = new Queue<Action<DbConnection, DbTransaction>>();
        //private readonly SortedSet<string> _lockedResources = new SortedSet<string>();
        private readonly SQLiteStorage _storage;        

        public SQLiteWriteOnlyTransaction([NotNull] SQLiteStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            _storage = storage;
        }

        public override void Commit()
        {
            _storage.UseTransaction((connection, transaction) =>
            {
                foreach (var command in _commandQueue)
                {
                    command(connection, transaction);
                }                
            });
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand((connection, transaction) => connection.Execute(
               $@"update [{_storage.SchemaName}.Job] set ExpireAt = @expireAt where Id = @id",
                new { expireAt = DateTime.UtcNow.Add(expireIn), id = long.Parse(jobId) },
                transaction));
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand((connection, transaction) => connection.Execute(
                $@"update [{_storage.SchemaName}.Job] set ExpireAt = NULL where Id = @id",
                new { id = long.Parse(jobId) },
                transaction));
        }

        public override void SetJobState(string jobId, IState state)
        {
            string addAndSetStateSql = 
$@"insert into [{_storage.SchemaName}.State] (JobId, Name, Reason, CreatedAt, Data)
values (@jobId, @name, @reason, @createdAt, @data);
update [{_storage.SchemaName}.Job] set StateId = last_insert_rowid(), StateName = @name where Id = @id;";

            QueueCommand((connection, transaction) => connection.Execute(
                addAndSetStateSql,
                new
                {
                    jobId = long.Parse(jobId),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = long.Parse(jobId)
                },
                transaction));
        }

        public override void AddJobState(string jobId, IState state)
        {
            string addStateSql = 
$@"insert into [{_storage.SchemaName}.State] (JobId, Name, Reason, CreatedAt, Data)
values (@jobId, @name, @reason, @createdAt, @data)";

            QueueCommand((connection, transaction) => connection.Execute(
                addStateSql,
                new
                {
                    jobId = long.Parse(jobId), 
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow, 
                    data = JobHelper.ToJson(state.SerializeData())
                },
                transaction));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            QueueCommand((connection, transaction) => persistentQueue.Enqueue(connection, queue, jobId));
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand((connection, transaction) => connection.Execute(
                $@"insert into [{_storage.SchemaName}.Counter] ([Key], [Value]) values (@key, @value)",
                new { key, value = +1 },
                transaction));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand((connection, transaction) => connection.Execute(
                $@"insert into [{_storage.SchemaName}.Counter] ([Key], [Value], [ExpireAt]) values (@key, @value, @expireAt)",
                new { key, value = +1, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand((connection, transaction) => connection.Execute(
                $@"insert into [{_storage.SchemaName}.Counter] ([Key], [Value]) values (@key, @value)",
                new { key, value = -1 },
                transaction));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand((connection, transaction) => connection.Execute(
                $@"insert into [{_storage.SchemaName}.Counter] ([Key], [Value], [ExpireAt]) values (@key, @value, @expireAt)",
                new { key, value = -1, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
//            string addSql = string.Format(@"
//;merge [{0}.Set] as Target
//using (VALUES (@key, @value, @score)) as Source ([Key], Value, Score)
//on Target.[Key] = Source.[Key] and Target.Value = Source.Value
//when matched then update set Score = Source.Score
//when not matched then insert ([Key], Value, Score) values (Source.[Key], Source.Value, Source.Score);", _storage.GetSchemaName());

            AcquireSetLock();
            QueueCommand((connection, transaction) =>
            {
                string tableName = $"[{_storage.SchemaName}.Set]";
                var selectSqlStr = $"select * from {tableName} where [Key] = @key and Value = @value";
                var insertSqlStr = $"insert into {tableName} ([Key], Value, Score) values (@key, @value, @score)";
                var updateSqlStr = $"update {tableName} set Score = @score where [Key] = @key and Value = @value";
                
                var fetchedSet = connection.Query<SqlSet>(selectSqlStr,
                    new { key = key, value = value }, transaction);
                if (!fetchedSet.Any())
                {
                    connection.Execute(insertSqlStr,
                        new { key = key, value, score }, transaction);
                }
                else
                {
                    connection.Execute(updateSqlStr,
                        new { key = key, value, score }, transaction);
                }                
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            string query = $@"delete from [{_storage.SchemaName}.Set] where [Key] = @key and Value = @value";

            AcquireSetLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, value },
                transaction));
        }

        public override void InsertToList(string key, string value)
        {
            AcquireListLock();
            QueueCommand((connection, transaction) => connection.Execute(
                $@"insert into [{_storage.SchemaName}.List] ([Key], Value) values (@key, @value);",
                new { key, value },
                transaction));
        }

        public override void RemoveFromList(string key, string value)
        {
            AcquireListLock();
            QueueCommand((connection, transaction) => connection.Execute(
                $@"delete from [{_storage.SchemaName}.List] where [Key] = @key and Value = @value",
                new { key, value },
                transaction));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
//            string trimSql = string.Format(@"
//;with cte as (
//    select row_number() over (order by Id desc) as row_num, [Key] 
//    from [{0}].List
//    where [Key] = @key)
//delete from cte where row_num not between @start and @end", _storage.GetSchemaName());

            string trimSql = 
$@"delete from [{_storage.SchemaName}.List] where [Key] = @key and Id not in (
  select Id from [{_storage.SchemaName}.List] where [Key] = @key order by Id desc limit @limit offset @offset)";

            AcquireListLock();
            QueueCommand((connection, transaction) => connection.Execute(
                trimSql,
                new { key = key, limit = keepEndingAt - keepStartingFrom + 1, offset = keepStartingFrom },
                transaction));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

//            string sql = string.Format(@"
//;merge [{0}.Hash] as Target
//using (VALUES (@key, @field, @value)) as Source ([Key], Field, Value)
//on Target.[Key] = Source.[Key] and Target.Field = Source.Field
//when matched then update set Value = Source.Value
//when not matched then insert ([Key], Field, Value) values (Source.[Key], Source.Field, Source.Value);", _storage.GetSchemaName());

            AcquireHashLock();
            QueueCommand((connection, transaction) =>
            {
                string tableName = $"[{_storage.SchemaName}.Hash]";
                var selectSqlStr = $"select * from {tableName} where [Key] = @key and Field = @field";
                var insertSqlStr = $"insert into {tableName} ([Key], Field, Value) values (@key, @field, @value)";
                var updateSqlStr = $"update {tableName} set Value = @value where [Key] = @key and Field = @field ";
                foreach (var keyValuePair in keyValuePairs)
                {
                    var fetchedHash = connection.Query<SqlHash>(selectSqlStr,
                        new { key = key, field = keyValuePair.Key }, transaction);
                    if (!fetchedHash.Any())
                    {
                        connection.Execute(insertSqlStr,
                            new { key = key, field = keyValuePair.Key, value = keyValuePair.Value }, transaction);
                    }
                    else
                    {
                        connection.Execute(updateSqlStr,
                            new { key = key, field = keyValuePair.Key, value = keyValuePair.Value }, transaction);
                    }
                }
            });
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"delete from [{_storage.SchemaName}.Hash] where [Key] = @key";

            AcquireHashLock();
            QueueCommand((connection, transaction) => connection.Execute(query, new { key }, transaction));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            string query = 
$@"insert into [{_storage.SchemaName}.Set] ([Key], Value, Score)
values (@key, @value, 0.0)";

            AcquireSetLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query,
                items.Select(value => new { key = key, value = value }).ToList(),
                transaction));
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"delete from [{_storage.SchemaName}.Set] where [Key] = @key";

            AcquireSetLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query, new { key = key }, transaction));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"update [{_storage.SchemaName}.Hash] set ExpireAt = @expireAt where [Key] = @key";

            AcquireHashLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"update [{_storage.SchemaName}.Set] set ExpireAt = @expireAt where [Key] = @key";

            AcquireSetLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"update [{_storage.SchemaName}.List] set ExpireAt = @expireAt where [Key] = @key";

            AcquireListLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"update [{_storage.SchemaName}.Hash] set ExpireAt = null where [Key] = @key";

            AcquireHashLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query, new { key = key }, transaction));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"update [{_storage.SchemaName}.Set] set ExpireAt = null where [Key] = @key";

            AcquireSetLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query, new { key = key }, transaction));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"update [{_storage.SchemaName}.List] set ExpireAt = null where [Key] = @key";

            AcquireListLock();
            QueueCommand((connection, transaction) => connection.Execute(
                query, new { key = key }, transaction));
        }

        internal void QueueCommand(Action<DbConnection, DbTransaction> action)
        {
            _commandQueue.Enqueue(action);
        }

        private void AcquireListLock()
        {
            AcquireLock("Hangfire:List:Lock");
        }

        private void AcquireSetLock()
        {
            AcquireLock("Hangfire:Set:Lock");
        }

        private void AcquireHashLock()
        {
            AcquireLock("Hangfire:Hash:Lock");
        }

        private void AcquireLock(string resource)
        {
            
        }
    }
}