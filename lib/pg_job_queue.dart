library pg_job_queue;

import 'package:postgres/postgres.dart';
import 'package:uuid/uuid.dart';

class PgJobQueue {
  PgJobQueue(
    this._connection, {
    String schema = 'default',
    String table = 'jobs',
    String defaultQueue = 'default',
    Duration defaultTimeout = const Duration(minutes: 30),
    String Function()? uniqueId,
  })  : _uniqueId = uniqueId ?? Uuid().v4,
        _defaultQueue = defaultQueue,
        _defaultTimeout = defaultTimeout,
        _insert = Sql.named('INSERT'
            ' INTO $schema.$table (id, queue, payload, status)'
            ' VALUES (@id, @queue, @payload, @status)'
            ' RETURNING id'),
        _fetch = Sql.named('SELECT * FROM $schema.$table WHERE id = @id'),
        _acquire = Sql.named('UPDATE $schema.$table SET'
            ' status = @new_status,'
            ' worker_id = @worker,'
            ' updated_at = now(),'
            ' processing_started_at = now(),'
            ' timeout = @timeout'
            ' WHERE id = (SELECT id FROM $schema.jobs WHERE queue = @queue and status = @status ORDER BY created_at LIMIT 1)'
            ' RETURNING *'),
        _complete = Sql.named('UPDATE $schema.$table SET'
            ' status = @status,'
            ' result = @result,'
            ' updated_at = now()'
            ' WHERE id = @id'),
        _init = [
          'CREATE TABLE IF NOT EXISTS $schema.$table ('
              ' id TEXT PRIMARY KEY,'
              ' queue TEXT NOT NULL,'
              ' payload JSONB NOT NULL,'
              ' status TEXT NOT NULL,'
              ' worker_id TEXT,'
              ' result JSONB,'
              ' created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,'
              ' updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,'
              ' processing_started_at TIMESTAMPTZ,'
              ' timeout INTERVAL'
              ' );'
        ];

  final Connection _connection;
  final Duration _defaultTimeout;
  final Sql _insert, _fetch, _acquire, _complete;
  final List<String> _init;
  final String Function() _uniqueId;
  final String _defaultQueue;

  /// Returns the list of SQL statements to initialize the job queue.
  List<String> get initStatements => _init.toList(growable: false);

  /// Initializes the job queue.
  Future<void> init() async {
    for (final sql in initStatements) {
      await _connection.execute(sql);
    }
  }

  Future<String> add(Map<String, Object?> payload, {String? queue}) async {
    final r = await _connection.execute(_insert, parameters: {
      'id': _uniqueId(),
      'queue': queue ?? _defaultQueue,
      'payload': payload,
      'status': Status.scheduled.toString(),
    });
    return r.first.first as String;
  }

  Future<Job?> fetch(String id) async {
    final r = await _connection.execute(_fetch, parameters: {'id': id});
    return r.isEmpty ? null : r.first.toJob();
  }

  Future<Job?> acquire(
      {String? queue, Duration? timeout, String? worker}) async {
    final r = await _connection.execute(_acquire, parameters: {
      'queue': queue ?? _defaultQueue,
      'worker': worker,
      'status': Status.scheduled.toString(),
      'new_status': Status.processing.toString(),
      'timeout': TypedValue(Type.interval, timeout ?? _defaultTimeout),
    });
    return r.isEmpty ? null : r.first.toJob();
  }

  Future<void> complete(String id,
      {Map<String, Object?> result = const {}}) async {
    await _connection.execute(_complete, parameters: {
      'id': id,
      'status': Status.completed.toString(),
      'result': result,
    });
  }
}

extension on Interval? {
  Duration? get duration =>
      this == null ? null : Duration(microseconds: this!.microseconds);
}

extension on ResultRow {
  Job toJob() => switch (toColumnMap()) {
        {
          'id': String id,
          'queue': String queue,
          'payload': Map<String, Object?> payload,
          'status': String status,
          'result': Map<String, Object?>? result,
          'timeout': Interval? timeout,
        } =>
          Job(id, queue, payload,
              Status.values.firstWhere((e) => e.toString() == status),
              result: result, timeout: timeout.duration),
        _ => throw StateError('Invalid row: $this'),
      };
}

class Job {
  Job(this.id, this.queue, this.payload, this.status,
      {this.result, this.timeout});

  final String id;
  final String queue;
  final Map<String, Object?> payload;
  final Map<String, Object?>? result;
  final Status status;
  final Duration? timeout;
}

enum Status { scheduled, processing, completed, failed }
