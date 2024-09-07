import 'dart:math';

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

  group('Happy path', () {
    test('completed', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      final id = await queue.schedule(payload);

      await queue.fetch(id).then((job) {
        expect(job, isNotNull);
        expect(job?.payload, equals(payload));
        expect(job?.queue, equals('default'));
        expect(job?.status, equals(JobStatus.scheduled));
        expect(job?.result, isNull);
        expect(job?.id, equals(id));
      });

      await queue.acquire(worker: 'worker1').then((job) {
        expect(job?.payload, equals(payload));
        expect(job?.queue, equals('default'));
        expect(job?.status, equals(JobStatus.acquired));
        expect(job?.result, isNull);
        expect(job?.id, equals(id));
      });

      await queue.complete(id);
      await queue.fetch(id).then((job) {
        expect(job?.payload, equals(payload));
        expect(job?.queue, equals('default'));
        expect(job?.status, equals(JobStatus.completed));
        expect(job?.result, equals({}));
        expect(job?.id, equals(id));
      });
    });

    test('failed', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      final id = await queue.schedule(payload);

      await queue.fetch(id).then((job) {
        expect(job, isNotNull);
        expect(job?.payload, equals(payload));
        expect(job?.queue, equals('default'));
        expect(job?.status, equals(JobStatus.scheduled));
        expect(job?.result, isNull);
        expect(job?.id, equals(id));
      });

      await queue.acquire(worker: 'worker1').then((job) {
        expect(job?.payload, equals(payload));
        expect(job?.queue, equals('default'));
        expect(job?.status, equals(JobStatus.acquired));
        expect(job?.result, isNull);
        expect(job?.id, equals(id));
      });

      await queue.fail(id);
      await queue.fetch(id).then((job) {
        expect(job?.payload, equals(payload));
        expect(job?.queue, equals('default'));
        expect(job?.status, equals(JobStatus.failed));
        expect(job?.result, equals({}));
        expect(job?.id, equals(id));
      });
    });
  });

  group('Argument validation', () {
    test('Schema must be alphanumeric with underscores', () async {
      final goodSchemas = ['test', 'test_1', 'test_1_2', 'fooBar', 'MOO'];
      final badSchemas = [
        'test-1',
        'test 1',
        'test!',
        'test@',
        'test#',
        '',
        '3'
      ];
      final connection = await createConnection();
      for (final schema in goodSchemas) {
        PgJobQueue(connection, schema: schema);
      }
      for (final schema in badSchemas) {
        expect(
          () => PgJobQueue(connection, schema: schema),
          throwsArgumentError,
        );
      }
    });

    test('Table must be alphanumeric with underscores', () async {
      final goodTables = ['test', 'test_1', 'test_1_2', 'fooBar', 'MOO'];
      final badTables = [
        'test-1',
        'test 1',
        'test!',
        'test@',
        'test#',
        '',
        '3'
      ];
      final connection = await createConnection();
      for (final table in goodTables) {
        PgJobQueue(connection, table: table);
      }
      for (final table in badTables) {
        expect(
          () => PgJobQueue(connection, table: table),
          throwsArgumentError,
        );
      }
    });

    test('Priority must be within range', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      expect(
        () => queue.schedule(payload, priority: PgJobQueue.minPriority - 1),
        throwsArgumentError,
      );
      expect(
        () => queue.schedule(payload, priority: PgJobQueue.maxPriority + 1),
        throwsArgumentError,
      );
    });
  });

  group('Status transition', () {
    test('Can not complete a scheduled job', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      final id = await queue.schedule(payload);
      expectLater(queue.complete(id), throwsStateError);
      expectLater(queue.fail(id), throwsStateError);
      final job = await queue.fetch(id);
      expect(job?.status, equals(JobStatus.scheduled));
    });
    test('Can not fail a scheduled job', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      final id = await queue.schedule(payload);
      expectLater(queue.complete(id), throwsStateError);
      expectLater(queue.fail(id), throwsStateError);
      final job = await queue.fetch(id);
      expect(job?.status, equals(JobStatus.scheduled));
    });
    test('Can not complete a completed job', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      final id = await queue.schedule(payload);
      await queue.acquire(worker: 'worker1');
      await queue.complete(id);
      expectLater(queue.complete(id), throwsStateError);
      expectLater(queue.fail(id), throwsStateError);
    });
    test('Can not complete a failed job', () async {
      final connection = await createConnection();
      final queue = PgJobQueue(connection, schema: schema);
      final payload = {'foo': 123, 'bar': 'baz.md'};
      final id = await queue.schedule(payload);
      await queue.acquire(worker: 'worker1');
      await queue.fail(id);
      expectLater(queue.complete(id), throwsStateError);
      expectLater(queue.fail(id), throwsStateError);
    });
  });

  test('Higher priority gets acquired first', () async {
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz.md'};
    for (var i = 0; i < 10; i++) {
      await queue.schedule(payload);
      await queue.schedule(payload, priority: 1);
      await queue.schedule(payload, priority: 2);
    }
    for (var p = 2; p >= 0; p--) {
      for (var i = 0; i < 10; i++) {
        await queue.acquire().then((job) {
          expect(job?.priority, equals(p));
          return queue.complete(job!.id);
        });
      }
    }
  });

  test('Can use min and max priority', () async {
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz.md'};
    await queue.schedule(payload, priority: PgJobQueue.minPriority);
    await queue.schedule(payload, priority: PgJobQueue.maxPriority);
    await queue.acquire().then((job) {
      expect(job?.priority, equals(PgJobQueue.maxPriority));
      return queue.complete(job!.id);
    });
    await queue.acquire().then((job) {
      expect(job?.priority, equals(PgJobQueue.minPriority));
      return queue.complete(job!.id);
    });
  });

  test('Can acquire a job with a specific queue', () async {
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz.md'};
    await queue.schedule(payload, queue: 'foo');
    await queue.schedule(payload, queue: 'bar');
    expect(await queue.acquire(), isNull);
    expect(await queue.acquire(queue: 'baz'), isNull);
    await queue.acquire(queue: 'foo').then((job) {
      expect(job?.queue, equals('foo'));
    });
    await queue.acquire(queue: 'bar').then((job) {
      expect(job?.queue, equals('bar'));
    });
  });

  test('Multiple workers', () async {
    const batchSize = 1000;
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz.md'};
    final ids = <String>[];
    final futures = <Future>[];
    for (var i = 0; i < batchSize; i++) {
      ids.add(await queue.schedule(payload));
      futures.add(Future.delayed(Duration(milliseconds: Random().nextInt(100)))
          .then((_) => queue.acquire(worker: 'worker$i'))
          .then((job) =>
              Future.delayed(Duration(milliseconds: Random().nextInt(100)))
                  .then((_) => queue.complete(job!.id))));
    }
    await Future.wait(futures);
    expect(await queue.acquire(), isNull);
    for (final id in ids) {
      final job = await queue.fetch(id);
      expect(job?.status, equals(JobStatus.completed));
    }
  });

  test('Count by queue by status', () async {
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz.md'};
    for (var i = 0; i < 5; i++) {
      await queue.schedule(payload);
      await queue.schedule(payload, queue: 'foo');
      await queue.schedule(payload, queue: 'bar');
      await queue.schedule(payload, queue: 'baz');
    }
    await queue.acquire(queue: 'foo').then((job) => queue.complete(job!.id));
    await queue.acquire(queue: 'foo').then((job) => queue.complete(job!.id));
    await queue.acquire(queue: 'foo').then((job) => queue.complete(job!.id));
    await queue.acquire(queue: 'foo').then((job) => queue.fail(job!.id));
    await queue.acquire(queue: 'foo').then((job) => queue.fail(job!.id));

    await queue.acquire(queue: 'bar').then((job) => queue.complete(job!.id));
    await queue.acquire(queue: 'bar').then((job) => queue.fail(job!.id));
    await queue.acquire(queue: 'bar');
    await queue.acquire(queue: 'baz');

    final counts = await queue.countByQueueByStatus();

    print('Status: "${JobStatus.acquired}"');
    expect(
        counts,
        equals({
          'bar': {
            'acquired': 1,
            'completed': 1,
            'failed': 1,
            'scheduled': 2
          },
          'baz': {'acquired': 1, 'scheduled': 4},
          'default': {'scheduled': 5},
          'foo': {'completed': 3, 'failed': 2}
        }));
  });
}
