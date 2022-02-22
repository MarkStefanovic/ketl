package ketl.service

import ketl.domain.JobResults
import ketl.domain.KETLError
import ketl.domain.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

fun CoroutineScope.heartbeat(
  log: Log,
  jobResults: JobResults,
  maxTimeToWait: Duration,
  timeBetweenChecks: Duration,
) = launch {
  while (isActive) {
    val latestResults = withTimeout(10.seconds) {
      jobResults.getLatestResults()
    }

    val latestEndTime = latestResults.maxOfOrNull { it.end }

    if (latestEndTime != null) {
      val secondsSinceLastResult = latestEndTime.until(LocalDateTime.now(), ChronoUnit.SECONDS)

      withTimeout(10.seconds) {
        log.info("The latest job results were received $secondsSinceLastResult seconds ago.")
      }

      if (secondsSinceLastResult > maxTimeToWait.inWholeSeconds) {
        log.info("Cancelling services...")
        throw KETLError.JobResultsStopped(secondsSinceLastResult.seconds)
      }
    }
    delay(timeBetweenChecks)
  }
}
