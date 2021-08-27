package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
@InternalCoroutinesApi
suspend fun jobStatusLogger(
  log: LogMessages,
  db: Db,
  repository: JobStatusRepository,
  status: SharedFlow<JobStatus>,
) = coroutineScope {
  status.collect { jobStatus ->
    try {
      db.exec { repository.upsert(jobStatus) }
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
