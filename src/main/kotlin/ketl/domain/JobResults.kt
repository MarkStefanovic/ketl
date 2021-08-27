package ketl.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.ExperimentalTime

@ExperimentalTime
class JobResults(
  private val scope: CoroutineScope,
  private val jobs: List<Job<*>>,
) {
  private val history = mutableMapOf<String, List<JobResult>>()

  private val mutex = Mutex()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 1000,
    )

  val stream = _stream.asSharedFlow()

  init {
    scope.launch {
      mutex.withLock { jobs.forEach { job -> history[job.name] = emptyList() } }
    }
  }

  suspend fun add(result: JobResult) {
    mutex.withLock {
      val priorResults = history[result.jobName]?.take(9) ?: emptyList()
      history[result.jobName] = priorResults + result
      scope.launch { _stream.emit(result) }
    }
  }

  suspend fun getHistoryForJob(jobName: String): List<JobResult> = mutex.withLock {
    history.getOrDefault(jobName, emptyList())
  }
}
