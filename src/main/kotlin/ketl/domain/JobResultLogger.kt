package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlin.coroutines.cancellation.CancellationException

@DelicateCoroutinesApi
suspend fun jobResultLogger(
  db: Db,
  results: SharedFlow<JobResult>,
  repository: ResultRepository,
  log: LogMessages,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  launch(dispatcher) {
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
}
