package ketl.domain

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class Schedule(
  val displayName: String,
  val parts: Set<SchedulePart>,
) {
  fun ready(refTime: LocalDateTime, lastRun: LocalDateTime?) =
    parts.any { part ->
      part.ready(refTime = refTime, lastRun = lastRun)
    }

  override fun toString() = """
    |Schedule [
    |  displayName: $displayName
    |  parts: 
    |    ${parts.map { it.toString().split("\n")}.joinToString("\n    ") }
    |]
  """.trimMargin()
}

@ExperimentalTime
internal fun durationHasElapsed(
  duration: Duration,
  lastRun: LocalDateTime?,
  refDateTime: LocalDateTime,
  startDateTime: LocalDateTime = LocalDateTime.MIN,
): Boolean {
  val latest = lastRun ?: startDateTime
  val secondsSinceLatest = latest.until(refDateTime, ChronoUnit.SECONDS)
  return secondsSinceLatest >= duration.inWholeSeconds
}
