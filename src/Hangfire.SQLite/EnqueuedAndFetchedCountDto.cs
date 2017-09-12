namespace Hangfire.SQLite
{
    public class EnqueuedAndFetchedCountDto
    {
        public long? EnqueuedCount { get; set; }
        public long? FetchedCount { get; set; }
    }
}
