package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
@InternalCoroutinesApi
suspend fun jobStatusLogger(
  db: Db,
  repository: JobStatusRepo,
  status: SharedFlow<JobStatus>,
) {
  status.collect { jobStatus ->
    try {
      db.exec { repository.upsert(jobStatus) }
    } catch (e: Exception) {
      if (e is CancellationException) {
        println("jobStatusLogger cancelled.")
      } else {
        e.printStackTrace()
      }
      throw e
    }
  }
}
