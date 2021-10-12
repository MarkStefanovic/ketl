package ketl.adapter

import ketl.domain.LogMessages
import ketl.domain.LogRepository
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun exposedLogRepositoryCleaner(
  db: Db,
  log: LogMessages,
  repository: LogRepository,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(3),
  timeout: Duration = Duration.minutes(15),
) {
  while (coroutineContext.isActive) {
    log.info("Cleaning up the log...")
    val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
    try {
      withTimeout(timeout) {
        db.exec { repository.deleteBefore(cutoff) }
        log.info("Finished cleaning up the log.")
      }
    } catch (e: Exception) {
      if (e is CancellationException) {
        println("exposedLogRepositoryCleaner cancelled.")
      } else {
        e.printStackTrace()
      }
      throw e
    }
    delay(timeBetweenCleanup.inWholeMilliseconds)
  }
}
