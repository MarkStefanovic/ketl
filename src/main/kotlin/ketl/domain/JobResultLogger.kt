package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlin.coroutines.cancellation.CancellationException

suspend fun jobResultLogger(
  db: Db,
  results: SharedFlow<JobResult>,
  repository: ResultRepository,
  log: LogMessages,
) = coroutineScope {
  results.collect { result ->
    try {
      db.exec {
        repository.add(result)
      }
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
