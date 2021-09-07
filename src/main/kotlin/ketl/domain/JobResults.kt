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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
class JobResults(
  private val db: Db,
  private val jobs: List<Job<*>>,
  scope: CoroutineScope,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) {
  private val latestResults = mutableMapOf<String, JobResult?>()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 1000,
    )

  val stream = _stream.asSharedFlow()

  private val mutex = Mutex()

  init {
    val repo = ExposedResultRepository()
    scope.launch(dispatcher) {
      jobs.forEach { job ->
        mutex.withLock {
          latestResults[job.name] = db.fetch {
            repo.getLatestResultsForJob(jobName = job.name, n = 1)
          }.firstOrNull()
        }
      }
    }
  }

  suspend fun add(result: JobResult) {
    mutex.withLock {
      latestResults[result.jobName] = result
    }
    _stream.emit(result)
  }

  suspend fun getLatestResultForJob(jobName: String) = mutex.withLock {
    latestResults[jobName]
  }
}
