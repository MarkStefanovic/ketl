package ketl.domain

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.util.concurrent.ConcurrentLinkedQueue
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

  private val jobs = ConcurrentLinkedQueue<KETLJob>(emptyList())

  override suspend fun add(job: KETLJob) {
    if (!jobs.contains(job)) {
      jobs.add(job)
      _stream.emit(jobs.toList())
    }
  }

  override suspend fun drop(jobName: String) {
    jobs.dropWhile { it.name == jobName }
    _stream.emit(jobs.toList())
  }

  override suspend fun pop(): KETLJob? {
    val job: KETLJob? = jobs.poll()
    _stream.emit(jobs.toList())
    return job
  }
}
