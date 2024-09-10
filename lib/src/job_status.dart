/// A job status.
enum JobStatus {
  /// The job is scheduled to be executed.
  scheduled,

  /// The job is acquired by a worker.
  acquired,

  /// The job is completed.
  completed,

  /// The job is failed.
  failed;
}
