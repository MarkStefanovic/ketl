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
  ) : JobResult(
    jobName = jobName,
    start = start,
    end = end,
  ) {
    override fun toString() = """
      |Cancelled [
      |  jobName: $jobName 
      |  start: $start
      |  end: $end
      |]
    """.trimMargin()
  }

  class Failed(
    jobName: String,
    start: LocalDateTime,
    end: LocalDateTime,
    val errorMessage: String,
  ) : JobResult(
    jobName = jobName,
    start = start,
    end = end,
  ) {
    override fun toString() = """
      |Failed [
      |  jobName: $jobName 
      |  start: $start
      |  end: $end
      |  errorMessage: $errorMessage
      |]
    """.trimMargin()
  }

  class Skipped(
    jobName: String,
    start: LocalDateTime,
    end: LocalDateTime,
    val reason: String,
  ) : JobResult(
    jobName = jobName,
    start = start,
    end = end,
  ) {
    override fun toString() = """
      |Skipped [
      |  jobName: $jobName 
      |  start: $start
      |  end: $end
      |  reason: $reason
      |]
    """.trimMargin()
  }

  class Success(
    jobName: String,
    start: LocalDateTime,
    end: LocalDateTime,
  ) : JobResult(
    jobName = jobName,
    start = start,
    end = end,
  ) {
    override fun toString() = """
      |Success [
      |  jobName: $jobName 
      |  start: $start
      |  end: $end
      |]
    """.trimMargin()
  }
}
