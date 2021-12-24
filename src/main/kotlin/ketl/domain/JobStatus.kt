package ketl.domain

import java.time.LocalDateTime

sealed class JobStatus(
  open val jobName: String,
  open val statusName: String,
  open val ts: LocalDateTime,
) {
  val isRunning: Boolean
    get() = this is Initial || this is Running

  data class Cancelled(override val jobName: String, override val ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "cancelled", ts = ts)

  data class Initial(override val jobName: String, override val ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "initial", ts = ts)

  data class Running(override val jobName: String, override val ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "running", ts = ts)

  data class Skipped(override val jobName: String, override val ts: LocalDateTime, val reason: String) :
    JobStatus(jobName = jobName, statusName = "skipped", ts = ts)

  data class Success(override val jobName: String, override val ts: LocalDateTime) :
    JobStatus(jobName = jobName, statusName = "successful", ts = ts)

  data class Failed(override val jobName: String, override val ts: LocalDateTime, val errorMessage: String) :
    JobStatus(jobName = jobName, statusName = "failed", ts = ts)
}
