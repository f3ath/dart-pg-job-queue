import 'package:pg_job_queue/src/job_status.dart';

/// A job model.
class Job {
  /// Creates a new job.
  /// - [id] is the job ID.
  /// - [queue] is the queue name.
  /// - [payload] is the job payload.
  /// - [status] is the job status.
  /// - [result] is the job result.
  /// - [worker] is the worker name.
  /// - [priority] is the job priority.
  Job(this.id, this.queue, this.payload, this.status,
      {this.result, this.worker, this.priority = 0});

  /// The job ID.
  final String id;

  /// The queue name.
  final String queue;

  /// The name of the worker processing the job.
  final String? worker;

  /// The job payload.
  final Map<String, Object?> payload;

  /// The job execution result.
  final Map<String, Object?>? result;

  /// The job status.
  final JobStatus status;

  /// The job priority.
  final int priority;
}
