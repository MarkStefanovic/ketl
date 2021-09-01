package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
class JobResults(
  private val jobs: List<Job<*>>,
  private val scope: CoroutineScope,
  private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
) {
  private val history = ConcurrentHashMap<String, List<JobResult>>()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 1000,
    )

  val stream = _stream.asSharedFlow()

  suspend fun start() = coroutineScope {
    launch(dispatcher) {
      jobs.forEach { job -> history[job.name] = emptyList() }
    }
  }

  fun add(result: JobResult) {
    val priorResults = history[result.jobName]?.take(9) ?: emptyList()
    history[result.jobName] = priorResults + result
    scope.launch { _stream.emit(result) }
  }

  fun getHistoryForJob(jobName: String): List<JobResult> =
    history.getOrDefault(jobName, emptyList())
}
