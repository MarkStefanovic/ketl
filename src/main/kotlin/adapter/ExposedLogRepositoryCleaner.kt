package main.kotlin.adapter

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import main.kotlin.domain.LogMessages
import main.kotlin.domain.LogRepository
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ExposedLogRepositoryCleaner(
  scope: CoroutineScope,
  private val db: Database,
  private val log: LogMessages,
  private val repository: LogRepository,
  private val timeBetweenCleanup: Duration = Duration.minutes(30),
  private val durationToKeep: Duration = Duration.days(3),
) {
  init {
    scope.launch {
      while (true) {
        log.info("Cleaning up the log...")
        val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
        transaction(db = db) {
          repository.deleteBefore(cutoff)
        }
        log.info("Finished cleaning up the log.")
        delay(timeBetweenCleanup.inWholeMilliseconds)
      }
    }
  }
}
