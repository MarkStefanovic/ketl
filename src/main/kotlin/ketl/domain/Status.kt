package ketl.domain

import java.time.LocalDateTime

sealed class Status(
  val statusName: JobStatusName,
  val ts: LocalDateTime,
) {
  data class Skipped(val reason: String) :
    Status(statusName = JobStatusName.Skipped, ts = LocalDateTime.now())

  object Success :
    Status(statusName = JobStatusName.Successful, ts = LocalDateTime.now())

  data class Failure(val errorMessage: String) :
    Status(statusName = JobStatusName.Failed, ts = LocalDateTime.now())
}
