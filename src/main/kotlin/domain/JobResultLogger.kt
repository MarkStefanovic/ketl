package main.kotlin.domain

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.coroutines.cancellation.CancellationException

suspend fun jobResultLogger(
  db: Database,
  results: SharedFlow<JobResult>,
  repository: ResultRepository,
  log: LogMessages,
) {
  results.collect { result ->
    try {
      transaction(db = db) { repository.add(result) }
    } catch (e: Exception) {
      if (e is CancellationException) {
        log.info("jobResultLogger cancelled.")
        throw e
      } else {
        log.error(e.stackTraceToString())
      }
    }
  }
}
