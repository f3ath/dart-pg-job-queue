# pg_job_queue
A simple job queue on top of PostgreSQL.

## Configuration
The only required configuration is the PostgreSQL connection object. The bare minimum configuration would look like this:

```dart
import 'package:pg_job_queue/pg_job_queue.dart';
import 'package:postgres/postgres.dart';

void main() async {
  /// To run locally, start postgres:
  /// `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres`
  final connection = await Connection.open(
      Endpoint(
        host: 'localhost',
        database: 'postgres',
        username: 'postgres',
        password: 'postgres',
      ),
      settings: ConnectionSettings(sslMode: SslMode.disable));

  /// Create a new job queue with default settings.
  final jobs = PgJobQueue(connection);

  /// Initialize the DB tables. This is idempotent.
  await jobs.init();
  
  /// At this point, the job queue is ready to use.
  
  /// Close the connection when done.
  await connection.close();
}
```
The code above creates a new job queue with default settings. The `init` method creates the necessary tables in the database,
and is idempotent, so it can be called multiple times without any issues. It is safe to call `init` every time the application starts.
When a new minor version is released, the `init` method is expected to automatically update the database schema to the new version.

By default, the job queue uses the `jobs` table in the `public` schema. The default queue name is `default`. 
This configuration can be changed by passing the relevant parameters to the `PgJobQueue` constructor.

The generated job ids are UUIDs v4. This can be changed by passing a custom `idGenerator` function to the `PgJobQueue` constructor.

## Schedule a job
To schedule a job, use the `schedule` method. It returns the id of the job that was scheduled.
Provide the JSON payload that will be passed to the job handler. You can also provide a custom `queue` if you want to use a different queue.
Each job may also be assigned a `priority` value. The default priority is `0`. Negative values are allowed. Jobs with higher priority values are executed first.

```dart
final jobId = await jobs.schedule(
  payload: {'key': 'value'},
  queue: 'default',
  priority: 0,
);
```

When a job is scheduled, it is inserted into the `jobs` table with the status `scheduled`.

## Check the status of a job
To check the status of a job, use the `fetch` method. It returns a `Job` object.

```dart
final job = await jobs.fetch(jobId);
print(job.status);
```

## Process jobs
To acquire the next job in the queue, use the `acquire` method. It returns a `Job` object.
You may provide a custom `queue` to acquire a job from a different queue. 
Another optional parameter is `worker`, which is an arbitrary string that identifies the worker that is processing the job.
The latter is useful for debugging and monitoring purposes.

After the processing is done, send the result to the job queue using either the `complete` or `fail` method.

```dart
final job = await jobs.acquire(queue: 'emails', worker: 'worker1');
print(job.payload);
await jobs.complete(job.id, result: {'key': 'value'});
```

## Clean up
To remove completed jobs from the database, use the `deleteCompleted` method. It removes all completed jobs that are older than the specified duration.
Use the `deleteFailed` parameter to remove the failed jobs as well. The `limit` can be specified to delete jobs in smaller chunks.
It is recommended to run this method periodically for better performance.

```dart
await jobs.deleteCompleted(Duration(days: 30), limit: 1000);
```

## Statistics
Some basic statistics are available through the `countByQueueByStatus` method. It returns a map with the number of jobs in each queue and status.

```dart
final stats = await jobs.countByQueueByStatus();
print(stats);
```

## Performance considerations
The job queue is designed to be simple and efficient. It uses a single database connection and a single transaction for each operation.
However, for larger volumes you may consider:
- Limiting the number or named queues. Queues sharing the same table affect each other's performance.
- Using a separate table for each queue. This can be achieved by passing custom `schema` and `table` to the `PgJobQueue` constructor.
- Using a separate database connection for each queue.
