package ketl.domain

import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class SchedulePart(
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
