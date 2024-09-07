library pg_job_queue;

import 'package:collection/collection.dart';
import 'package:postgres/postgres.dart';
import 'package:uuid/uuid.dart';

class PgJobQueue {
  PgJobQueue(
    this._connection, {
    String schema = 'default',
    String table = 'jobs',
    String defaultQueue = 'default',
    String Function()? uniqueId,
  })  : _uniqueId = uniqueId ?? Uuid().v4,
        _defaultQueue = defaultQueue,
        _insert = Sql.named('''
INSERT
INTO "$schema"."$table" (id, queue, payload, priority, status)
VALUES (@id, @queue, @payload, @priority, '${JobStatus.scheduled.name}')
RETURNING id'''
            .trim()),
        _fetch = Sql.named('''
SELECT * FROM "$schema"."$table" WHERE id = @id'''
            .trim()),
        _acquire = Sql.named('''
UPDATE "$schema"."$table" SET
  status = '${JobStatus.acquired.name}',
  worker = @worker,
  updated_at = now()
WHERE id = (
  SELECT id FROM "$schema"."$table"
  WHERE 
    queue = @queue 
    AND status = '${JobStatus.scheduled.name}'
  ORDER BY priority DESC, created_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *'''
            .trim()),
        _complete = Sql.named('''
UPDATE "$schema"."$table" SET
  status = @status,
  result = @result,
  updated_at = now()
WHERE 
  id = @id
  AND status = '${JobStatus.acquired.name}'
'''
            .trim()),
        _countByQueueByStatus = Sql.named('''
SELECT queue, status, count(*) as job_count 
FROM "$schema"."$table"
GROUP BY queue, status 
ORDER BY queue, status'''
            .trim()),
        _init = [
          '''
CREATE TABLE IF NOT EXISTS "$schema"."$table" (
  id text PRIMARY KEY,
  queue text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL,
  priority smallint NOT NULL,
  worker text,
  result jsonb,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);''',
          '''
CREATE INDEX idx_${schema}_${table}_queue_status_priority_created_at
ON "$schema"."$table" (queue, status, priority DESC, created_at);
'''
        ].map((s) => s.trim()).toList() {
    if (!_schemaRegExp.hasMatch(schema)) {
      throw ArgumentError.value(
          schema, 'schema', 'Schema name must match $_schemaRegExp');
    }
    if (!_tableRegExp.hasMatch(table)) {
      throw ArgumentError.value(
          table, 'table', 'Table name must match $_tableRegExp');
    }
  }

  static const maxPriority = 32767;
  static const minPriority = -32768;

  static final _schemaRegExp =
      RegExp(r'^[a-z_][a-z0-9_]*$', caseSensitive: false);

  static final _tableRegExp =
      RegExp(r'^[a-z_][a-z0-9_]*$', caseSensitive: false);

  final Connection _connection;
  final Sql _insert, _fetch, _acquire, _complete, _countByQueueByStatus;
  final List<String> _init;
  final String Function() _uniqueId;
  final String _defaultQueue;

  /// Returns the list of SQL statements to initialize the job queue.
  List<String> get initStatements => [..._init];

  /// Initializes the job queue.
  Future<void> init() async {
    for (final sql in initStatements) {
      await _connection.execute(sql);
    }
  }

  Future<String> schedule(Map<String, Object?> payload,
      {String? queue, int priority = 0}) async {
    if (priority < minPriority || priority > maxPriority) {
      throw ArgumentError.value(priority, 'priority',
          'Priority must be between $minPriority and $maxPriority');
    }
    final r = await _connection.execute(_insert, parameters: {
      'id': _uniqueId(),
      'queue': queue ?? _defaultQueue,
      'payload': payload,
      'priority': priority,
    });
    return r.first.first as String;
  }

  Future<Job?> fetch(String id) async {
    final r = await _connection.execute(_fetch, parameters: {'id': id});
    return r.isEmpty ? null : r.first.toJob();
  }

  Future<Job?> acquire({String? queue, String? worker}) async {
    final r = await _connection.execute(_acquire, parameters: {
      'queue': queue ?? _defaultQueue,
      'worker': worker,
    });
    return r.isEmpty ? null : r.first.toJob();
  }

  Future<void> complete(String id,
      {Map<String, Object?> result = const {}, bool failed = false}) async {
    final r = await _connection.execute(_complete, parameters: {
      'id': id,
      'status': (failed ? JobStatus.failed : JobStatus.completed).name,
      'result': result,
    });
    if (r.affectedRows != 1) {
      throw StateError('Job not found or not in processing state: $id');
    }
  }

  Future<void> fail(String id, {Map<String, Object?> result = const {}}) =>
      complete(id, result: result, failed: true);

  Future<Map<String, Map<String, int>>> countByQueueByStatus() async {
    final r = await _connection.execute(_countByQueueByStatus);
    return groupBy(r.map((r) => r.toColumnMap()), (r) => r['queue'] as String)
        .map((queue, rows) => MapEntry(
            queue,
            lastBy(rows, (r) => r['status'] as String)
                .map((status, row) =>
                    MapEntry(status, (row['job_count'] as int).toInt()))
                .cast<String, int>()));
  }
}

extension on ResultRow {
  Job toJob() => switch (toColumnMap()) {
        {
          'id': String id,
          'queue': String queue,
          'worker': String? worker,
          'payload': Map<String, Object?> payload,
          'priority': int priority,
          'status': String status,
          'result': Map<String, Object?>? result,
        } =>
          Job(id, queue, payload,
              JobStatus.values.firstWhere((e) => e.name == status),
              result: result, worker: worker, priority: priority),
        _ => throw StateError('Invalid row: $this'),
      };
}

class Job {
  Job(this.id, this.queue, this.payload, this.status,
      {this.result, this.worker, this.priority = 0});

  final String id;
  final String queue;
  final String? worker;
  final Map<String, Object?> payload;
  final Map<String, Object?>? result;
  final JobStatus status;
  final int priority;
}

enum JobStatus { scheduled, acquired, completed, failed }
