using Hangfire.Storage;
using System.Data;
using System.Threading;

namespace Hangfire.SQLite
{
    public interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
        void Enqueue(IDbConnection connection, string queue, string jobId);
    }
}
