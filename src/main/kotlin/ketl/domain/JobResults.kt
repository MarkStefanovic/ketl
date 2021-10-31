package ketl.domain

import ketl.adapter.Db
import ketl.adapter.DbResultRepo
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
class JobResults(private val db: Db) {
  private val repo = DbResultRepo()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 1000,
    )

  val stream = _stream.asSharedFlow()

  suspend fun add(result: JobResult) {
    _stream.emit(result)
  }

  fun getLatestResultForJob(jobName: String): JobResult? {
    return db.fetch {
      repo.getLatestResultsForJob(jobName = jobName, n = 1)
    }.firstOrNull()
  }
}
