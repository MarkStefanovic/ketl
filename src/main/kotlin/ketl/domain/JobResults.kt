package ketl.domain

import ketl.adapter.Db
import ketl.adapter.ExposedResultRepository
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
class JobResults(
  private val db: Db,
  private val jobs: List<Job<*>>,
  private val scope: CoroutineScope,
  private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
) {
  val latestResults = ConcurrentHashMap<String, JobResult>()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 1000,
    )

  val stream = _stream.asSharedFlow()

  init {
    val repo = ExposedResultRepository()
    scope.launch(dispatcher) {
      jobs.forEach { job ->
        latestResults[job.name] = db.fetch {
          repo.getLatestResultsForJob(jobName = job.name, n = 1)
        }.first()
      }
    }
  }

  fun add(result: JobResult) {
    latestResults[result.jobName] = result
    scope.launch(dispatcher) { _stream.emit(result) }
  }

//  fun latestResult(jobName: String): JobResult? =
//    latestResults[jobName]
}
