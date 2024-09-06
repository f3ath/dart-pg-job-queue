import 'package:pg_job_queue/pg_job_queue.dart';
import 'package:test/expect.dart';
import 'package:test/scaffolding.dart';

import 'create_connection.dart';

/// To run locally, start postgres:
/// `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres`
void main() async {
  const schema = 'test';
  setUp(() async {
    final connection = await createConnection();
    await connection.execute('DROP SCHEMA IF EXISTS $schema CASCADE;');
    await connection.execute('CREATE SCHEMA IF NOT EXISTS $schema;');
    await PgJobQueue(connection, schema: schema).init();
  });

  test('Happy path', () async {
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz.md'};
    final id = await queue.add(payload);

    await queue.fetch(id).then((it) {
      expect(it?.payload, equals(payload));
      expect(it?.queue, equals('default'));
      expect(it?.status, equals(Status.scheduled));
      expect(it?.result, isNull);
      expect(it?.timeout, isNull);
      expect(it?.id, equals(id));
    });

    await queue
        .acquire(worker: 'worker1', timeout: Duration(seconds: 30))
        .then((it) {
      expect(it?.payload, equals(payload));
      expect(it?.queue, equals('default'));
      expect(it?.status, equals(Status.processing));
      expect(it?.result, isNull);
      expect(it?.timeout, equals(Duration(seconds: 30)));
      expect(it?.id, equals(id));
    });

    await queue.complete(id);
    await queue.fetch(id).then((it) {
      expect(it?.payload, equals(payload));
      expect(it?.queue, equals('default'));
      expect(it?.status, equals(Status.completed));
      expect(it?.result, equals({}));
      expect(it?.timeout, equals(Duration(seconds: 30)));
      expect(it?.id, equals(id));
    });
  });
}
