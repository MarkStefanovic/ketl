package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
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
    } catch (te: TimeoutCancellationException) {
      println(
        """
      |jobStatusLogger: 
      |  db.exec timed out while attempting to add the following job status:
      |    $jobStatus
      |  message: ${te.message}
      """.trimMargin()
      )
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
