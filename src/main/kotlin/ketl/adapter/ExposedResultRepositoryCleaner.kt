package ketl.adapter

import ketl.domain.LogMessages
import ketl.domain.ResultRepository
import kotlinx.coroutines.delay
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun exposedResultRepositoryCleaner(
  db: Db,
  log: LogMessages,
  repository: ResultRepository,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(3),
) {
  while (true) {
    log.info("Cleaning up the job results log...")
    val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
    db.exec { repository.deleteBefore(cutoff) }
    log.info("Finished cleaning up the job results log.")
    delay(timeBetweenCleanup.inWholeMilliseconds)
  }
}
