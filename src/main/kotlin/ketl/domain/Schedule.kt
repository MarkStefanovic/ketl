package ketl.domain

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class Schedule(
  val displayName: String,
  val frequency: Duration,
  val window: ExecutionWindow = ExecutionWindow.ANYTIME,
  val startDateTime: LocalDateTime = LocalDateTime.MIN,
) {
  fun ready(refTime: LocalDateTime, lastRun: LocalDateTime?): Boolean =
    window.inWindow(refTime) &&
      durationHasElapsed(
        duration = frequency,
        lastRun = lastRun,
        startDateTime = startDateTime,
        refDateTime = refTime,
      )
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
