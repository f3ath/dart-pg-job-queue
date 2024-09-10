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

  /// Schedule some jobs.
  for (var i = 0; i < 10; i++) {
    await jobs.schedule({'name': 'Job $i'});
  }

  await Future.wait(['Alice', 'Bob', 'Chris'].map((worker) async {
    print('Worker $worker started');
    while (true) {
      final job = await jobs.acquire(worker: worker);
      if (job == null) {
        print('All done. $worker shutting down');
        break;
      }
      print('Worker $worker processing job ${job.id} (${job.payload})');
      await jobs.complete(job.id);
    }
  }));
  await connection.close();
}
