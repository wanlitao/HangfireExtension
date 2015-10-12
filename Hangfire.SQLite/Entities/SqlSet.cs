using System;

namespace Hangfire.SQLite.Entities
{
    internal class SqlSet
    {        
        public int Id { get; set; }
        
        public string Key { get; set; }

        public double Score { get; set; }

        public string Value { get; set; }
        
        public DateTime? ExpireAt { get; set; }
    }
}
