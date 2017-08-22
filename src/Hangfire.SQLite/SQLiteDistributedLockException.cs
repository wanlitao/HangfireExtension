using System;

namespace Hangfire.SQLite
{
    public class SQLiteDistributedLockException : Exception
    {
        public SQLiteDistributedLockException(string message)
            : base(message)
        {
        }
    }
}
