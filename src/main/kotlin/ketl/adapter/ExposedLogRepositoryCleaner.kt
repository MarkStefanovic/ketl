package ketl.adapter

import ketl.domain.LogMessages
import ketl.domain.LogRepository
import kotlinx.coroutines.delay
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun exposedLogRepositoryCleaner(
  db: Db,
  log: LogMessages,
  repository: LogRepository,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(3),
) {
  while (true) {
    log.info("Cleaning up the log...")
    val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
    db.exec { repository.deleteBefore(cutoff) }
    log.info("Finished cleaning up the log.")
    delay(timeBetweenCleanup.inWholeMilliseconds)
  }
}
