import 'package:pg_job_queue/pg_job_queue.dart';
import 'package:test/expect.dart';
import 'package:test/scaffolding.dart';

import 'create_connection.dart';

/// To run locally, start postgres:
/// `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres`
void main() async {
  setUp(() async {
    final connection = await createConnection();
    await connection.execute('DROP SCHEMA IF EXISTS $schema CASCADE;');
    await connection.execute('CREATE SCHEMA IF NOT EXISTS $schema;');
    for (final sql in init) {
      await connection.execute(sql);
    }
  });

  test('Happy path', () async {
    final connection = await createConnection();
    final queue = PgJobQueue(connection, schema: schema);
    final payload = {'foo': 123, 'bar': 'baz'};
    final id = await queue.add('my_type', payload);
    final job = await queue.fetch(id);
    expect(job?.payload, equals(payload));
    expect(job?.type, equals('my_type'));
    expect(job?.status, equals(Status.scheduled));
    expect(job?.result, isNull);
    expect(job?.timeout, isNull);
    expect(job?.id, isNotEmpty);
  });
}

const schema = 'job_queue';
const init = [
  'CREATE EXTENSION IF NOT EXISTS "pgcrypto";',
  '''
CREATE TABLE IF NOT EXISTS $schema.jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT,
    result JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processing_started_at TIMESTAMPTZ,
    timeout INTERVAL DEFAULT INTERVAL '30 minutes'
);
'''
];
