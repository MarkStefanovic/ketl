package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
@InternalCoroutinesApi
suspend fun jobStatusLogger(
  log: LogMessages,
  db: Db,
  repository: JobStatusRepository,
  status: SharedFlow<JobStatus>,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  launch(dispatcher) {
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
}
