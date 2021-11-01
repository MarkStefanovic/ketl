package ketl.adapter

import ketl.domain.ETLLog
import ketl.domain.ResultRepo
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
suspend fun dbResultRepoCleaner(
  db: Db,
  log: ETLLog,
  repository: ResultRepo,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(7),
  timeout: Duration = Duration.minutes(15),
) {
  while (coroutineContext.isActive) {
    log.info("Cleaning up the job results log...")
    val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
    try {
      withTimeout(timeout) {
        db.exec { repository.deleteBefore(cutoff) }
        log.info("Finished cleaning up the job results log.")
      }
    } catch (e: Exception) {
      if (e is CancellationException) {
        println("exposedResultRepositoryCleaner cancelled.")
      } else {
        e.printStackTrace()
      }
      throw e
    }
    delay(timeBetweenCleanup.inWholeMilliseconds)
  }
}