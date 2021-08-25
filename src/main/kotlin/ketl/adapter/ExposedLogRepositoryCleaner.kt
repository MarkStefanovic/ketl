package ketl.adapter

import ketl.domain.LogMessages
import ketl.domain.LogRepository
import kotlinx.coroutines.delay
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun exposedLogRepositoryCleaner(
  db: Database,
  log: LogMessages,
  repository: LogRepository,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(3),
) {
  while (true) {
    log.info("Cleaning up the log...")
    val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
    transaction(db = db) { repository.deleteBefore(cutoff) }
    log.info("Finished cleaning up the log.")
    delay(timeBetweenCleanup.inWholeMilliseconds)
  }
}
