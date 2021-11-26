package ketl.domain

import java.time.LocalDateTime

sealed class JobStatus(
  val jobName: String,
  val statusName: String,
  val ts: LocalDateTime,
) {
  class Cancelled(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "cancelled", ts = ts)

  class Initial(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "initial", ts = ts)

  class Running(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "running", ts = ts)

  class Skipped(jobName: String, ts: LocalDateTime, val reason: String) :
    JobStatus(jobName = jobName, statusName = "skipped", ts = ts)

  class Success(jobName: String, ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "successful", ts = ts)

  class Failed(jobName: String, ts: LocalDateTime, val errorMessage: String) :
    JobStatus(jobName = jobName, statusName = "failed", ts = ts)
}
