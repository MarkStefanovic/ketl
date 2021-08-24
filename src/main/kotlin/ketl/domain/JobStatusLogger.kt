package main.kotlin.ketl.domain

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
@InternalCoroutinesApi
suspend fun jobStatusLogger(
  log: LogMessages,
  db: Database,
  repository: JobStatusRepository,
  status: SharedFlow<JobStatus>,
) {
  status.collect { jobStatus ->
    try {
      transaction(db = db) { repository.upsert(jobStatus) }
    } catch (e: Exception) {
      if (e is CancellationException) {
        log.info("jobStatusLogger cancelled.")
        throw e
      } else {
        log.error(e.stackTraceToString())
      }
    }
  }
}
