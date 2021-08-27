package ketl.domain

import java.time.LocalDateTime
import java.time.ZoneOffset

sealed class JobResult(
  val jobName: String,
  val start: LocalDateTime,
  val end: LocalDateTime,
) {
  val executionSeconds: Long
    get() = end.toEpochSecond(ZoneOffset.UTC) - start.toEpochSecond(ZoneOffset.UTC)

  class Cancelled(
    jobName: String,
    start: LocalDateTime,
    end: LocalDateTime,
  ) :
    JobResult(
      jobName = jobName,
      start = start,
      end = end,
    )

  class Failure(
    jobName: String,
    start: LocalDateTime,
    end: LocalDateTime,
    val errorMessage: String,
  ) :
    JobResult(
      jobName = jobName,
      start = start,
      end = end,
    )

  class Success(
    jobName: String,
    start: LocalDateTime,
    end: LocalDateTime,
  ) :
    JobResult(
      jobName = jobName,
      start = start,
      end = end,
    )
}
