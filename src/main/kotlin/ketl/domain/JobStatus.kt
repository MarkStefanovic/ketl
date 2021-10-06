package ketl.domain

import java.time.LocalDateTime

sealed class JobStatus(
  val jobName: String,
  val statusName: JobStatusName,
  val ts: LocalDateTime,
) {
  class Cancelled(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Cancelled, ts = ts)

  class Initial(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Initial, ts = ts)

  class Running(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Running, ts = ts)

  class Skipped(jobName: String, ts: LocalDateTime, val reason: String) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Skipped, ts = ts)

  class Success(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Successful, ts = ts)

  class Failure(jobName: String, ts: LocalDateTime, val errorMessage: String) :
    JobStatus(jobName = jobName, statusName = JobStatusName.Failed, ts = ts)
}
