package ketl.domain

import java.time.LocalDateTime

sealed class Status(
  val statusName: String,
  val ts: LocalDateTime,
) {
  data class Skipped(val reason: String) :
    Status(statusName = "skipped", ts = LocalDateTime.now())

  object Success :
    Status(statusName = "successful", ts = LocalDateTime.now())

  data class Failure(val errorMessage: String) :
    Status(statusName = "failed", ts = LocalDateTime.now())
}
