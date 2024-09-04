library pg_job_queue;

import 'package:postgres/postgres.dart';

class PgJobQueue {
  PgJobQueue(this._connection, {String schema = 'default'})
      : _insert = Sql.named('insert'
            ' into $schema.jobs (type, payload, status)'
            ' values (@type, @payload, @status)'
            ' returning id'),
        _fetch = Sql.named(
            'select type, payload, status from $schema.jobs where id = @id');
  final Connection _connection;
  final Sql _insert;
  final Sql _fetch;

  Future<String> add(String type, Map<String, Object?> payload) async {
    final r = await _connection.execute(_insert, parameters: {
      'type': type,
      'payload': payload,
      'status': Status.scheduled.toString(),
    });
    return r.first.first as String;
  }

  Future<Job?> fetch(String id) async {
    final r = await _connection.execute(_fetch, parameters: {'id': id});
    if (r.isEmpty) return null;
    final row = r.first.toColumnMap();
    return Job(
      id,
      row['type'],
      row['payload'],
      Status.values.firstWhere((e) => e.toString() == row['status']),
    );
  }
}

class Job {
  Job(this.id, this.type, this.payload, this.status,
      {this.result, this.timeout});

  final String id;
  final String type;
  final Map<String, Object?> payload;
  final Map<String, Object?>? result;
  final Status status;
  final Duration? timeout;
}

enum Status { scheduled, processing, done, failed }
