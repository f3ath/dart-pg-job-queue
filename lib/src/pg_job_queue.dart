import 'package:collection/collection.dart';
import 'package:migrant/migrant.dart';
import 'package:migrant/testing.dart';
import 'package:migrant_db_postgresql/migrant_db_postgresql.dart';
import 'package:pg_job_queue/src/job.dart';
import 'package:pg_job_queue/src/job_status.dart';
import 'package:postgres/postgres.dart';
import 'package:uuid/uuid.dart';

/// A job queue backed by PostgreSQL.
class PgJobQueue {
  /// Creates a new PostgreSQL job queue.
  ///  - [connection] is the PostgreSQL connection.
  ///  - [schema] is the schema name for the job queue table.
  ///  - [table] is the table name for the job queue table.
  ///  - [defaultQueue] is the default queue name for jobs.
  ///  - [uniqueId] is a function to generate unique job IDs. By default, it
  ///   uses [Uuid.v4].
  PgJobQueue(
    this._connection, {
    String schema = 'public',
    String table = 'jobs',
    String defaultQueue = 'default',
    String Function()? uniqueId,
  })  : _uniqueId = uniqueId ?? Uuid().v4,
        _defaultQueue = defaultQueue,
        _database = Database(PostgreSQLGateway(_connection,
            schema: schema, tablePrefix: '_${table}_migrations')),
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
        _deleteOld = Sql.named('''
DELETE FROM "$schema"."$table"
WHERE id IN (
  SELECT id FROM "$schema"."$table"
  WHERE
    status = ANY(@statuses)
    AND updated_at < (now() - @ttl::interval)
  LIMIT @limit
 );
'''
            .trim()),
        _migrations = [
          Migration('0001', [
            '''
CREATE TABLE "$schema"."$table" (
  id text PRIMARY KEY,
  queue text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL,
  priority smallint NOT NULL,
  worker text,
  result jsonb,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);'''
                .trim(),
            '''
CREATE INDEX "idx_${schema}_${table}_queue_status_priority_created_at"
ON "$schema"."$table" (queue, status, priority DESC, created_at);'''
                .trim(),
            '''
CREATE INDEX "idx_${schema}_${table}updated_at_status_"
ON "$schema"."$table" (updated_at, status);'''
                .trim(),
          ])
        ] {
    if (!_schemaRegExp.hasMatch(schema)) {
      throw ArgumentError.value(
          schema, 'schema', 'Schema name must match $_schemaRegExp');
    }
    if (!_tableRegExp.hasMatch(table)) {
      throw ArgumentError.value(
          table, 'table', 'Table name must match $_tableRegExp');
    }
  }

  /// The maximum job priority.
  static const maxPriority = 32767;

  /// The minimum job priority.
  static const minPriority = -32768;

  static final _schemaRegExp =
      RegExp(r'^[a-z_][a-z0-9_]*$', caseSensitive: false);

  static final _tableRegExp =
      RegExp(r'^[a-z_][a-z0-9_]*$', caseSensitive: false);

  final Connection _connection;
  final Sql _insert,
      _fetch,
      _acquire,
      _complete,
      _countByQueueByStatus,
      _deleteOld;
  final List<Migration> _migrations;
  final String Function() _uniqueId;
  final String _defaultQueue;
  final Database _database;

  /// Initializes the job queue.
  Future<void> init() => _database.upgrade(InMemory(_migrations));

  /// Schedules a job to be executed.
  /// - [payload] is the job payload.
  /// - [queue] is the queue name. If not provided, it uses the default queue.
  /// - [priority] is the job priority. It must be between [minPriority] and
  ///  [maxPriority]. By default, it is 0. Jobs with higher priority are
  ///  processed first.
  ///
  ///  Returns the job ID.
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

  /// Fetches a job by ID.
  ///
  /// Returns the job or `null` if not found.
  Future<Job?> fetch(String id) async {
    return await _connection.execute(_fetch,
        parameters: {'id': id}).then((r) => r.isEmpty ? null : r.first.toJob());
  }

  /// Acquires a job to be processed.
  /// - [queue] is the queue name. If not provided, it uses the default queue.
  /// - [worker] is the worker name.
  ///
  /// Returns the job or `null` if no job is available.
  Future<Job?> acquire({String? queue, String? worker}) =>
      _connection.execute(_acquire, parameters: {
        'queue': queue ?? _defaultQueue,
        'worker': worker,
      }).then((r) => r.isEmpty ? null : r.first.toJob());

  /// Completes a job by setting its status to completed or failed and setting
  /// the result.
  /// - [id] is the job ID.
  /// - [result] is the job result.
  /// - [fail] sets the status to [JobStatus.failed] instead of [JobStatus.completed]..
  /// Throws a [StateError] if the job is not found or not in processing state.
  Future<void> complete(String id,
          {Map<String, Object?> result = const {}, bool fail = false}) =>
      _connection.execute(_complete, parameters: {
        'id': id,
        'status': (fail ? JobStatus.failed : JobStatus.completed).name,
        'result': result,
      }).then((r) {
        if (r.affectedRows != 1) {
          throw StateError('Job not found or not in processing state: $id');
        }
      });

  /// Marks a job as failed
  /// - [id] is the job ID.
  /// - [result] is the job result.
  /// Throws a [StateError] if the job is not found or not in processing state.
  Future<void> fail(String id, {Map<String, Object?> result = const {}}) =>
      complete(id, result: result, fail: true);

  /// Returns some basic stats: a map of queue names to a map of status names to job counts.
  Future<Map<String, Map<String, int>>> countByQueueByStatus() =>
      _connection.execute(_countByQueueByStatus).then((r) =>
          groupBy(r.map((r) => r.toColumnMap()), (r) => r['queue'] as String)
              .map((queue, rows) => MapEntry(
                  queue,
                  lastBy(rows, (r) => r['status'] as String).map(
                      (status, row) =>
                          MapEntry(status, row['job_count'] as int)))));

  /// Deletes completed jobs older than the given TTL.
  /// - [ttl] is the time past the last update.
  /// - use [includeFailed] to include failed jobs.
  /// - [limit] is the number of jobs to delete in a single transaction.
  /// Returns the number of jobs deleted.
  Future<int> deleteCompleted(Duration ttl,
          {bool includeFailed = false, int limit = 1000}) =>
      _connection.execute(_deleteOld, parameters: {
        'statuses': TypedValue(Type.textArray, [
          JobStatus.completed.name,
          if (includeFailed) JobStatus.failed.name,
        ]),
        'ttl': TypedValue(Type.interval, Interval.duration(ttl)),
        'limit': limit,
      }).then((r) => r.affectedRows);
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
        // coverage:ignore-line
        _ => throw StateError('Invalid row: $this'),
      };
}
