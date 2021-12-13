package ketl.domain

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import kotlin.time.ExperimentalTime

@ExperimentalTime
interface JobQueue {
  val stream: StateFlow<List<KETLJob>>

  suspend fun add(job: KETLJob)

  suspend fun drop(jobName: String)

  suspend fun pop(): KETLJob?
}

@ExperimentalTime
object DefaultJobQueue : JobQueue {
  private val _stream = MutableStateFlow<List<KETLJob>>(emptyList())
  override val stream = _stream.asStateFlow()

  private val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

  private val jobs = mutableListOf<KETLJob>()

  override suspend fun add(job: KETLJob) = withContext(dispatcher) {
    if (!jobs.contains(job)) {
      jobs.add(job)
      _stream.emit(jobs.toList())
    }
  }

  override suspend fun drop(jobName: String) = withContext(dispatcher) {
    jobs.dropWhile { it.name == jobName }
    _stream.emit(jobs.toList())
  }

  override suspend fun pop(): KETLJob? = withContext(dispatcher) {
    val job = if (jobs.isEmpty()) {
      null
    } else {
      jobs.removeFirst()
    }
    _stream.emit(jobs.toList())
    job
  }
}
