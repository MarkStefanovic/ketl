package main.kotlin.adapter

import kotlinx.coroutines.delay
import main.kotlin.domain.LogMessages
import main.kotlin.domain.ResultRepository
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun exposedResultRepositoryCleaner(
  db: Database,
  log: LogMessages,
  repository: ResultRepository,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(3),
) {
  while (true) {
    log.info("Cleaning up the job results log...")
    val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
    transaction(db = db) { repository.deleteBefore(cutoff) }
    log.info("Finished cleaning up the job results log.")
    delay(timeBetweenCleanup.inWholeMilliseconds)
  }
}
