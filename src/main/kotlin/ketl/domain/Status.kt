package ketl.domain

import java.time.LocalDateTime

sealed class Status(
  val statusName: String,
  val ts: LocalDateTime,
) {
  data class Skipped(val reason: String) :
    Status(statusName = "skipped", ts = LocalDateTime.now()) {
    override fun toString(): String =
      """
      |Status [
      |  reason: $reason
      |  ts: $ts
      |]
      """.trimMargin()
  }

  object Success :
    Status(statusName = "successful", ts = LocalDateTime.now()) {
    override fun toString(): String =
      """
      |Success [
      |  ts: $ts
      |]
      """.trimMargin()
  }

  data class Failure(val errorMessage: String) :
    Status(statusName = "failed", ts = LocalDateTime.now()) {
    override fun toString(): String =
      """
      |Failure [
      |  errorMessage: $errorMessage
      |  ts: $ts
      |]
      """.trimMargin()
  }
}
